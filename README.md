# uast2clickhouse

The CLI tool to put [Babelfish's UASTs](https://docs.sourced.tech/babelfish/uast/uast-specification-v2) to [ClickHouse DB](https://clickhouse.yandex).

It is written in Go and has zero dependencies. The list of solved problems includes:

- Normalizing the UAST even stronger than in the Semantic mode.
- Converting a tree structure to a linear list of "interesting" nodes.
- Handling runtime errors which are typical to big data processing: OOMs, crashes, DB insertion failures, etc.
- Running distributed and unattended.

### Installation

You need a [Go compiler >=1.11](https://golang.org/).

```
export GO111MODULE=on
go build uast2clickhouse
```

### Usage

Install ClickHouse >= 19.4 and initialize the DB schema:

```
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
) ENGINE = MergeTree() ORDER BY (repo, file, id);

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
) ENGINE = MergeTree() ORDER BY repo;"
```

Then run on each of the nodes

```
./uast2clickhouse --heads heads.csv --db default:password@10.150.0.9/default /path/to/parquet
```

or

```
./uast2clickhouse --heads heads.csv --db default:password@10.150.0.9/default 10.150.0.9:11300
```

`heads.csv` contain the mapping from the HEAD UUIDs in Parquet to the actual repository names. If you
work with [PGA](https://github.com/src-d/datasets/tree/master/PublicGitArchive), [download it](https://drive.google.com/open?id=136vsGWfIwfd0IrAdfphIU6lkMmme4-Pj) or [generate with list-pga-heads](https://github.com/src-d/datasets/tree/master/PublicGitArchive/list-pga-heads).
`--db default:password@10.150.0.9/default` is the ClickHouse connection string.
`10.150.0.9:11300` is a sample [beanstalkd](https://github.com/beanstalkd/beanstalkd) message queue address for distributed processing. 
You should specify `--read-streams` and `--db-streams` to reach the peak performance. `--read-streams` sets the number of
goroutines to read the Parquet file, and `--db-streams`  set the number of HTTP threads which upload the SQL insertions to ClickHouse.
Usually `--db-streams` is bigger than `--read-streams`. The bigger values increase the memory pressure.

### Sample operation

Input: UASTs extracted from PGA'19, 204068 Parquet files overall in a 6 TB Google Cloud volume.
DB instance configuration: Google Cloud "highcpu" with 64 cores, 58GB of RAM. 6 local NVMe SSDs joined in RAID0 and formatted to ext4 with journal disabled. Ubuntu 18.04.
Worker configuration: Ubuntu 18.04 with 20GB of SSD disk and the UASTs volume attached read-only at `/mnt/uasts`.

- Install and run `beanstalkd` on the DB instance. Build locally and `scp` there the [`beanstool` binary](https://github.com/src-d/beanstool).
- List all the Parquet files with `find /mnt/uasts -name '*.parquet' | gzip > tasks.gz` on one of the workers.
- `scp tasks.gz` to the DB instance. `zcat tasks.gz | xargs -n1 ./beanstool put --ttr 1000h -t default -b` to fill the queue.
- Install and setup ClickHouse on the DB instance. There are sample [`/etc/clickhouse-server/config.xml`](config.xml) and [`/etc/clickhouse-server/users.xml`](users.xml).
- Execute the pushing procedure in 4 stages.

1. 16 workers, 2 cores, 4 GB RAM each. `./uast2clickhouse --read-streams 2 --db-streams 6 --heads heads.csv --db default:password@10.150.0.9/default 10.150.0.9:11300`. This succeeds with ~80% of the tasks. Then `./beanstool kick --num NNN -t default`.
2. 16 workers, 2 cores, 4 GB RAM each. `./uast2clickhouse --read-streams 1 --db-streams 1 --heads heads.csv --db default:password@10.150.0.9/default 10.150.0.9:11300`. This succeeds with all but 1k tasks.
3. 16 workers, 2 cores, 16 GB RAM each ("highmem"). Same command. This leaves only ~10 tasks.
4. 2 workers, 4 cores, 32 GB RAM each ("highmem"). Same command, full success.

- Create the secondary DB indexes.

```
SET allow_experimental_data_skipping_indices = 1;
ALTER TABLE uasts ADD INDEX lang lang TYPE set(0) GRANULARITY 1;
ALTER TABLE uasts ADD INDEX type type TYPE set(0) GRANULARITY 1;
ALTER TABLE uasts ADD INDEX value_exact value TYPE bloom_filter() GRANULARITY 1;
ALTER TABLE uasts ADD INDEX left (repo, file, left) TYPE minmax GRANULARITY 1;
ALTER TABLE uasts ADD INDEX right (repo, file, right) TYPE minmax GRANULARITY 1;
ALTER TABLE uasts MATERIALIZE INDEX lang;
ALTER TABLE uasts MATERIALIZE INDEX type;
ALTER TABLE uasts MATERIALIZE INDEX value_exact;
ALTER TABLE uasts MATERIALIZE INDEX left;
ALTER TABLE uasts MATERIALIZE INDEX right;
OPTIMIZE TABLE uasts FINAL;
```

The whole thing takes ~1 week.

### Tests

There are sadly no tests at the moment. We are going to fix this.

### License

Apache 2.0, see [LICENSE](LICENSE).

