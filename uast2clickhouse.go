package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beanstalkd/go-beanstalk"
	"github.com/mailru/dbr"
	"github.com/mailru/go-clickhouse"
	"github.com/spf13/pflag"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"gopkg.in/bblfsh/sdk.v2/uast"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes/nodesproto"
	progress "gopkg.in/cheggaaa/pb.v1"
)

func parseFlags() (inputDef, dbAddr, table, headMap string, dbstreams, readstreams int) {
	pflag.IntVar(&readstreams, "read-streams", runtime.NumCPU(), "Number of concurrent Parquet reader streams.")
	pflag.IntVar(&dbstreams, "db-streams", runtime.NumCPU(), "Number of concurrent DB insertion streams.")
	pflag.StringVar(&dbAddr, "db", "0.0.0.0:8123/default", "ClickHouse endpoint.")
	pflag.StringVar(&table, "table", "uasts", "ClickHouse table name.")
	pflag.StringVar(&headMap, "heads", "", "HEAD UUID mapping to repository names in CSV.")
	pflag.Parse()
	if pflag.NArg() != 1 {
		log.Fatalf("usage: uast2clickhouse /path/to/file.parquet")
	}
	inputDef = pflag.Arg(0)
	return
}

const (
	batchSize = 2000
)

type parquetItem struct {
	Head string `parquet:"name=head, type=UTF8"`
	Path string `parquet:"name=path, type=UTF8"`
	UAST string `parquet:"name=uast, type=BYTE_ARRAY"`
}

func createProgressBar(total int) *progress.ProgressBar {
	bar := progress.New(total)
	bar.Start()
	bar.ShowPercent = false
	bar.ShowSpeed = false
	bar.ShowElapsedTime = true
	bar.ShowFinalTime = false
	return bar
}

func processParquet(parquetPath string, streams int, payload func(head, path string, tree nodes.Node)) {
	fr, err := local.NewLocalFileReader(parquetPath)
	if err != nil {
		log.Fatalf("Can't open %s: %v", parquetPath, err)
	}
	defer fr.Close()
	pr, err := reader.NewParquetReader(fr, new(parquetItem), int64(runtime.NumCPU()))
	if err != nil {
		log.Fatalf("Can't read %s: %v", parquetPath, err)
	}
	defer pr.ReadStop()
	jobs := make(chan parquetItem, streams)
	wg := sync.WaitGroup{}
	wg.Add(streams)
	seenPaths := make(map[string]struct{})
	seenPathsMutex := sync.RWMutex{}
	for i := 0; i < streams; i++ {
		go func(i int) {
			for item := range jobs {
				if tree, err := nodesproto.ReadTree(bytes.NewBuffer([]byte(item.UAST))); err != nil {
					log.Fatalf("Item %d deserialize error %s: %v", i+1, parquetPath, err)
				} else {
					seenPathsMutex.Lock()
					if _, ok := seenPaths[item.Path]; !ok {
						seenPaths[item.Path] = struct{}{}
						seenPathsMutex.Unlock()
						payload(item.Head, item.Path, tree)
					} else {
						seenPathsMutex.Unlock()
					}
				}
			}
			wg.Done()
		}(i)
	}
	nrows := int(pr.GetNumRows())
	bar := createProgressBar(nrows)
	defer bar.Finish()
	for i := 0; i < nrows; i++ {
		item := make([]parquetItem, 1)
		if err = pr.Read(&item); err != nil {
			log.Fatalf("Item %d read error %s: %v", i+1, parquetPath, err)
		}
		jobs <- item[0]
		bar.Add(1)
	}
	close(jobs)
	wg.Wait()
}

type walkTrace struct {
	Node            nodes.External
	Path            []int32
	ParentKey       string
	DiscardedTypes  []string
	ParentPositions uast.Positions
}

/*

clickhouse-client --query="CREATE TABLE uasts (
  id Int32,
  left Int32,
  right Int32,
  repo String,
  lang String,
  file String,
  line Int32,
  parents Array(Int32),
  pkey String,
  roles Array(Int16),
  type String,
  orig_type String,
  uptypes Array(String),
  value String
) ENGINE = MergeTree() ORDER BY (repo, file, id);"

CREATE TABLE meta (
   repo String,
   siva_filenames Array(String),
   file_count Int32,
   langs Array(String),
   langs_bytes_count Array(UInt32),
   langs_lines_count Array(UInt32),
   langs_files_count Array(UInt32),
   commits_count Int32,
   branches_count Int32,
   forks_count Int32,
   empty_lines_count Array(UInt32),
   code_lines_count Array(UInt32),
   comment_lines_count Array(UInt32),
   license_names Array(String),
   license_confidences Array(Float32),
   stars Int32,
   size Int64,
   INDEX stars stars TYPE minmax GRANULARITY 1
) ENGINE = MergeTree() ORDER BY repo;
*/

type record struct {
	ID            int32  `db:"id"`
	LeftID        int32  `db:"left"`
	RightID       int32  `db:"right"`
	Repository    string `db:"repo"`
	Language      string `db:"lang"`
	File          string `db:"file"`
	Line          int32  `db:"line"`
	Parents       string `db:"parents"` // []int32
	ParentKey     string `db:"pkey"`
	Roles         string `db:"roles"` // []int16
	Type          string `db:"type"`
	OriginalType  string `db:"orig_type"`
	UpstreamTypes string `db:"uptypes"` // []string
	Value         string `db:"value"`
}

type positionedRecord struct {
	Record record
	Column int32
}

func formatArray(v interface{}) string {
	val, _ := clickhouse.Array(v).Value()
	return string(val.([]byte))
}

func emitRecords(repo, file string, tree nodes.Node, emitter chan record) {
	lang := strings.Split(uast.TypeOf(tree), ":")[0]
	queue := []walkTrace{{tree, nil, "", nil, nil}}
	var i int32 = 1
	var positionedRecords []positionedRecord
	for len(queue) > 0 {
		trace := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		node := trace.Node
		path := trace.Path
		parentPs := trace.ParentPositions
		nodeOriginalType := uast.TypeOf(node)

		goDeeper := func(discarded bool, ps uast.Positions) {
			var discardedTypes []string
			if discarded {
				nodeType := uast.TypeOf(node)
				if nodeType != "" {
					discardedTypes = make([]string, len(trace.DiscardedTypes)+1)
					copy(discardedTypes, trace.DiscardedTypes)
					discardedTypes[len(trace.DiscardedTypes)] = nodeType
				}
			}
			switch nodes.KindOf(node) {
			case nodes.KindObject:
				if n, ok := node.(nodes.ExternalObject); ok {
					for _, k := range n.Keys() {
						if !isAttributeBlacklisted(k, lang) {
							v, _ := n.ValueAt(k)
							queue = append(queue, walkTrace{v, path, k, discardedTypes, ps})
						}
					}
				}
			case nodes.KindArray:
				if n, ok := node.(nodes.ExternalArray); ok {
					for j := 0; j < n.Size(); j++ {
						v := n.ValueAt(j)
						queue = append(queue, walkTrace{v, path, strconv.Itoa(j), discardedTypes, ps})
					}
				}
			}
		}

		var ps uast.Positions
		value := ""
		var roles []int16
		if properNode, casted := node.(nodes.Node); casted {
			ps = uast.PositionsOf(properNode)
			value = uast.TokenOf(properNode)
			rroles := uast.RolesOf(properNode)
			roles = make([]int16, len(rroles))
			for i, r := range rroles {
				roles[i] = int16(r)
			}
		} else {
			goDeeper(true, parentPs)
			continue
		}
		if ps.Start() == nil || ps.Start().Line == 0 {
			if parentPs != nil {
				ps = parentPs
			} else {
				goDeeper(true, parentPs)
				continue
			}
		}
		if _, exists := typesToGoDeeper[nodeOriginalType]; exists {
			goDeeper(true, ps)
			continue
		}
		if value == "" {
			if properNode, casted := node.(nodes.Object); casted {
				if sub := properNode["Name"]; sub != nil {
					value = fmt.Sprint(sub.Value())
				} else if sub := properNode["name"]; sub != nil {
					value = fmt.Sprint(sub.Value())
				} else if sub := properNode["Text"]; sub != nil {
					value = fmt.Sprint(sub.Value())
				} else if sub := properNode["text"]; sub != nil {
					value = fmt.Sprint(sub.Value())
				} else if sub := properNode["Value"]; sub != nil {
					value = fmt.Sprint(sub.Value())
				} else if sub := properNode["value"]; sub != nil {
					value = fmt.Sprint(sub.Value())
				} else if sub := properNode["Names"]; sub != nil {
					if n, ok := sub.(nodes.ExternalArray); ok {
						var parts []string
						for j := 0; j < n.Size(); j++ {
							v := n.ValueAt(j)
							if iv, ok := v.(nodes.Object); ok {
								if nn := iv["Name"]; nn != nil {
									parts = append(parts, fmt.Sprint(nn.Value()))
								}
							}
						}
						value = strings.Join(parts, "/")
					}
				}
			}
		}
		if value == "" || value == "<nil>" {
			// empty value => we don't care
			goDeeper(true, ps)
			continue
		}
		nodeType := nodeOriginalType
		if properClass, exists := typeMapping[nodeOriginalType]; exists {
			nodeType = properClass
		}
		path = make([]int32, len(trace.Path)+1)
		copy(path, trace.Path)
		path[len(trace.Path)] = i
		goDeeper(false, ps)
		positionedRecords = append(positionedRecords, positionedRecord{
			Record: record{
				ID:            i,
				LeftID:        0,
				RightID:       0,
				Repository:    repo,
				Language:      lang,
				File:          file,
				Line:          int32(ps.Start().Line),
				Parents:       formatArray(trace.Path),
				ParentKey:     trace.ParentKey,
				Roles:         formatArray(roles),
				Type:          nodeType,
				OriginalType:  nodeOriginalType,
				UpstreamTypes: formatArray(trace.DiscardedTypes),
				Value:         value,
			},
			Column: int32(ps.Start().Col),
		})
		i++
	}
	sort.Slice(positionedRecords, func(i, j int) bool {
		if positionedRecords[i].Record.Line == positionedRecords[j].Record.Line {
			if positionedRecords[i].Column == positionedRecords[j].Column {
				return i < j
			} else {
				return positionedRecords[i].Column < positionedRecords[j].Column
			}
		} else {
			return positionedRecords[i].Record.Line < positionedRecords[j].Record.Line
		}
	})
	var prev_id int32 = 0
	for j, positionedRecord := range positionedRecords {
		record := positionedRecord.Record
		record.LeftID = prev_id
		prev_id = record.ID
		if j+1 < len(positionedRecords) {
			record.RightID = positionedRecords[j+1].Record.ID
		}
		emitter <- record
	}
}

func readHeadMap(path string) map[string]string {
	rawReader, err := os.Open(path)
	if err != nil {
		log.Fatalf("Failed to read %s: %v", path, err)
	}
	defer rawReader.Close()
	csvReader := csv.NewReader(rawReader)
	records, err := csvReader.ReadAll()
	if err != nil {
		log.Fatalf("Failed to read/parse %s: %v", path, err)
	}
	result := map[string]string{}
	for _, r := range records {
		result[r[0]] = r[1]
	}
	return result
}

func createParquetPathGenerator(inputDef string) (chan string, func(string)) {
	gen := make(chan string, 1)
	_, err := os.Stat(inputDef)
	if err == nil {
		gen <- inputDef
		close(gen)
		return gen, func(string) {}
	}
	if strings.Count(inputDef, ":") != 1 {
		log.Fatalf("Unreadable input: %s: %v", inputDef, err)
	}
	conn, err := beanstalk.Dial("tcp", inputDef)
	if err != nil {
		log.Fatalf("Cannot connect to beanstalkd at %s: %v", inputDef, err)
	}
	jobMap := map[string]uint64{}
	lock := &sync.Mutex{}

	deleteJob := func(path string) {
		lock.Lock()
		id := jobMap[path]
		lock.Unlock()
		if err = conn.Delete(id); err != nil {
			log.Fatalf("Cannot delete job %d in beanstalkd: %v", id, err)
		}
		lock.Lock()
		delete(jobMap, path)
		lock.Unlock()
	}

	go func() {
		defer conn.Close()
		for {
			id, body, err := conn.Reserve(time.Hour)
			if err != nil {
				log.Fatalf("Cannot read from beanstalkd at %s: %v", inputDef, err)
			}
			path := string(body)
			if len(path) > 0 {
				lock.Lock()
				jobMap[path] = id
				lock.Unlock()
				gen <- path
			} else {
				close(gen)
				break
			}
		}
	}()
	return gen, deleteJob
}

func main() {
	log.Println(os.Args)
	inputDef, dbAddr, table, headMap, dbStreams, readStreams := parseFlags()
	heads := readHeadMap(headMap)
	log.Printf("Read %d head mappings", len(heads))
	conn, err := dbr.Open("clickhouse", "http://"+dbAddr, nil)
	if err != nil {
		log.Fatalf("Failed to open %s: %v", dbAddr, err)
	}
	if err := conn.Ping(); err != nil {
		log.Fatalf("Failed to open %s: %v", dbAddr, err)
	}
	conn.SetMaxOpenConns(dbStreams)
	log.Printf("Connected to %s", dbAddr)
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error while closing the DB connection: %v", err)
		}
	}()

	gen, onDone := createParquetPathGenerator(inputDef)
	for parquetPath := range gen {
		pushParquet(parquetPath, dbStreams, conn, table, heads, readStreams)
		onDone(parquetPath)
	}
}

func pushParquet(parquetPath string, dbStreams int, conn *dbr.Connection, table string,
	heads map[string]string, readStreams int) {

	jobs := make(chan record, 2*dbStreams)
	wg := sync.WaitGroup{}
	for x := 0; x < dbStreams; x++ {
		wg.Add(1)
		go func() {
			session := conn.NewSession(nil)
			// Do not close the session! It nukes the whole database.
			defer wg.Done()

			newBuilder := func() *dbr.InsertBuilder {
				return session.InsertInto(table).
					Columns("id", "left", "right", "repo", "lang", "file", "line", "parents", "pkey", "roles", "type", "orig_type", "uptypes", "value")
			}
			builder := newBuilder()

			submit := func() {
				values := [][][]interface{}{builder.Value}
				bisected := 1
				pushedSizes := make([]int, 0, 1)
				for bisected > 0 {
					bisected = 0
					for i, value := range values {
						batchBuilder := newBuilder()
						batchBuilder.Value = value
						_, err := batchBuilder.Exec()
						if err == nil {
							pushedSizes = append(pushedSizes, len(value))
							values[i] = nil
						} else if strings.Contains(err.Error(), "Cannot parse expression") {
							bisected++
						} else {
							log.Fatalf("Failed to insert: %v", err)
						}
					}
					if bisected > 0 {
						newValues := make([][][]interface{}, 0, bisected*2)
						for _, value := range values {
							if value != nil {
								if len(value) > 1 {
									p1, p2 := value[:len(value)/2], value[len(value)/2:]
									newValues = append(newValues, p1)
									newValues = append(newValues, p2)
								} else {
									log.Fatalf("Failed to insert values: %s", value)
								}
							}
						}
						values = newValues
					}
				}
				if len(pushedSizes) > 1 {
					log.Printf("We had to bisect the batch: %v", pushedSizes)
				}
				builder = newBuilder()
			}

			i := 1
			for r := range jobs {
				if i%batchSize == 0 {
					submit()
					i = 0
				}
				builder.Record(r)
				i++
			}
			submit()
		}()
	}
	handleFile := func(head, path string, tree nodes.Node) {
		emitRecords(heads[head], path, tree, jobs)
	}
	log.Printf("Reading %s", parquetPath)
	processParquet(parquetPath, readStreams, handleFile)
	log.Println("Finishing")
	close(jobs)
	wg.Wait()
}
