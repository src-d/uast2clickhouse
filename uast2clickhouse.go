package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

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

func parseFlags() (parquetPath, dbAddr, table, headMap string, dbstreams, readstreams int) {
	pflag.IntVar(&readstreams, "read-streams", runtime.NumCPU(), "Number of concurrent Parquet reader streams.")
	pflag.IntVar(&dbstreams, "db-streams", runtime.NumCPU(), "Number of concurrent DB insertion streams.")
	pflag.StringVar(&dbAddr, "db", "0.0.0.0:8123/default", "ClickHouse endpoint.")
	pflag.StringVar(&table, "table", "uasts", "ClickHouse table name.")
	pflag.StringVar(&headMap, "heads", "", "HEAD UUID mapping to repository names in CSV.")
	pflag.Parse()
	if pflag.NArg() != 1 {
		log.Fatalf("usage: uast2clickhouse /path/to/file.parquet")
	}
	parquetPath = pflag.Arg(0)
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

func readParquet(parquetPath string, streams int, payload func(head, path string, tree nodes.Node)) {
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
  repo String,
  lang String,
  file String,
  line Int32,
  parents Array(Int32),
  pkey String,
  roles Array(Int16),
  type String,
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
	Repository    string `db:"repo"`
	Language      string `db:"lang"`
	File          string `db:"file"`
	Line          int32  `db:"line"`
	Parents       string `db:"parents"` // []int32
	ParentKey     string `db:"pkey"`
	Roles         string `db:"roles"` // []int16
	Type          string `db:"type"`
	UpstreamTypes string `db:"uptypes"` // []string
	Value         string `db:"value"`
}

func formatArray(v interface{}) string {
	val, _ := clickhouse.Array(v).Value()
	return string(val.([]byte))
}

func emitRecords(repo, file string, tree nodes.Node, emitter chan record) {
	lang := strings.Split(uast.TypeOf(tree), ":")[0]
	queue := []walkTrace{{tree, nil, "", nil, nil}}
	var i int32 = 1
	for len(queue) > 0 {
		trace := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		node := trace.Node
		path := trace.Path
		parentPs := trace.ParentPositions
		nodeType := uast.TypeOf(node)

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
						if _, ok := attributeBlacklist[k]; !ok {
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
		if nodeType == "" {
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
					// https://github.com/bblfsh/go-driver/issues/55
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

		path = make([]int32, len(trace.Path)+1)
		copy(path, trace.Path)
		path[len(trace.Path)] = i
		goDeeper(false, ps)

		emitter <- record{
			ID:            i,
			Repository:    repo,
			Language:      lang,
			File:          file,
			Line:          int32(ps.Start().Line),
			Parents:       formatArray(trace.Path),
			ParentKey:     trace.ParentKey,
			Roles:         formatArray(roles),
			Type:          nodeType,
			UpstreamTypes: formatArray(trace.DiscardedTypes),
			Value:         value,
		}
		i++
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

func main() {
	log.Println(os.Args)
	parquetPath, dbAddr, table, headMap, dbstreams, readstreams := parseFlags()
	heads := readHeadMap(headMap)
	log.Printf("Read %d head mappings", len(heads))
	conn, err := dbr.Open("clickhouse", "http://"+dbAddr, nil)
	if err != nil {
		log.Fatalf("Failed to open %s: %v", dbAddr, err)
	}
	if err := conn.Ping(); err != nil {
		log.Fatalf("Failed to open %s: %v", dbAddr, err)
	}
	conn.SetMaxOpenConns(dbstreams)
	log.Printf("Connected to %s", dbAddr)
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error while closing the DB connection: %v", err)
		}
	}()
	jobs := make(chan record, 2*dbstreams)
	wg := sync.WaitGroup{}
	for x := 0; x < dbstreams; x++ {
		wg.Add(1)
		go func() {
			session := conn.NewSession(nil)
			// Do not close the session! It nukes the whole database.
			defer wg.Done()

			newBuilder := func() *dbr.InsertBuilder {
				return session.InsertInto(table).
					Columns("id", "repo", "lang", "file", "line", "parents", "pkey", "roles", "type", "uptypes", "value")
			}
			builder := newBuilder()

			submit := func() {
				values := [][][]interface{}{builder.Value}
				bisected := 1
				for bisected > 0 {
					bisected = 0
					for i, value := range values {
						batchBuilder := newBuilder()
						batchBuilder.Value = value
						_, err := batchBuilder.Exec()
						if err == nil {
							values[i] = nil
						} else if strings.Contains(err.Error(), "Cannot parse expression") {
							log.Printf("Batch is too big, bisecting: %d rows, current queue size: %d",
								len(value), len(values))
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
									log.Printf("New batch sizes: %d + %d", len(p1), len(p2))
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
	readParquet(parquetPath, readstreams, handleFile)
	log.Println("Finishing")
	close(jobs)
	wg.Wait()
}
