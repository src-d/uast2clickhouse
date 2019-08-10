package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
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

func parseFlags() (parquetPath, dbAddr, table, headMap string, dbstreams, readstreams, batch int) {
	pflag.IntVar(&readstreams, "read-streams", runtime.NumCPU(), "Number of concurrent Parquet reader streams.")
	pflag.IntVar(&dbstreams, "db-streams", runtime.NumCPU(), "Number of concurrent DB insertion streams.")
	pflag.StringVar(&dbAddr, "db", "0.0.0.0:8123/default", "ClickHouse endpoint.")
	pflag.StringVar(&table, "table", "uasts", "ClickHouse table name.")
	pflag.IntVar(&batch, "batch", 2000, "INSERT batch size.")
	pflag.StringVar(&headMap, "heads", "", "HEAD UUID mapping to repository names in CSV.")
	pflag.Parse()
	if pflag.NArg() != 1 {
		log.Fatalf("usage: uast2clickhouse /path/to/file.parquet")
	}
	parquetPath = pflag.Arg(0)
	return
}

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
	for i := 0; i < streams; i++ {
		go func(i int) {
			for item := range jobs {
				if tree, err := nodesproto.ReadTree(bytes.NewBuffer([]byte(item.UAST))); err != nil {
					log.Fatalf("Item %d deserialize error %s: %v", i+1, parquetPath, err)
				} else {
					payload(item.Head, item.Path, tree)
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
	Node      nodes.External
	Path      []int32
	ParentKey string
}

/*

CREATE TABLE uasts (
  id Int32,
  repo String,
  file String,
  line Int32,
  parents Array(Int32),
  pkey String,
  roles Array(Int16),
  type String,
  value String
) ENGINE = MergeTree() ORDER BY (repo, file, id);

*/

type record struct {
	ID         int32         `db:"id"`
	Repository string        `db:"repo"`
	File       string        `db:"file"`
	Line       int32         `db:"line"`
	Parents    string        `db:"parents"`  // []int32
	ParentKey  string        `db:"pkey"`
	Roles      string        `db:"roles"`  // []int16
	Type       string        `db:"type"`
	Value      string        `db:"value"`
}

func formatArray(v interface{}) string {
	val, _ := clickhouse.Array(v).Value()
	return string(val.([]byte))
}

func emitRecords(repo, file string, tree nodes.Node, emitter chan record) {
	queue := []walkTrace{{tree, nil, ""}}
	var i int32 = 1
	for len(queue) > 0 {
		trace := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		node := trace.Node
		path := trace.Path

		goDeeper := func() {
			switch nodes.KindOf(node) {
			case nodes.KindObject:
				if n, ok := node.(nodes.ExternalObject); ok {
					for _, k := range n.Keys() {
						v, _ := n.ValueAt(k)
						queue = append(queue, walkTrace{v, path, k})
					}
				}
			case nodes.KindArray:
				if n, ok := node.(nodes.ExternalArray); ok {
					for j := 0; j < n.Size(); j++ {
						v := n.ValueAt(j)
						queue = append(queue, walkTrace{v, path, strconv.Itoa(j)})
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
			goDeeper()
			continue
		}
		if ps.Start() == nil || ps.Start().Line == 0 {
			goDeeper()
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
				}
			}
		}
		if value == "" || value == "<nil>" {
			// empty value => we don't care
			goDeeper()
			continue
		}

		path = make([]int32, len(trace.Path)+1)
		copy(path, trace.Path)
		path[len(trace.Path)] = i
		goDeeper()

		emitter <- record{
			ID:         i,
			Repository: repo,
			File:       file,
			Line:       int32(ps.Start().Line),
			Parents:    formatArray(trace.Path),
			ParentKey:  trace.ParentKey,
			Roles:      formatArray(roles),
			Type:       uast.TypeOf(node),
			Value:      value,
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
	parquetPath, dbAddr, table, headMap, dbstreams, readstreams, batch := parseFlags()
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

			newBuilder := func() *dbr.InsertBuilder{
				return session.InsertInto(table).
					Columns("id", "repo", "file", "line", "parents", "pkey", "roles", "type", "value")
			}
			builder := newBuilder()

			submit := func() {
				_, err := builder.Exec()
				if err != nil {
					log.Fatalf("Failed to insert: %v", err)
				}
				builder = newBuilder()
			}

			i := 1
			for r := range jobs {
				if i % batch == 0 {
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
