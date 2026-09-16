# GitHub Event Lakehouse: PrestoDB · Apache Hudi · MinIO · Airflow

A self-contained **data lakehouse** that runs entirely in Docker. It ingests the public
[GitHub Archive](https://www.gharchive.org/) event firehose into ACID-transactional
[Apache Hudi](https://hudi.apache.org/) tables on S3-compatible object storage, registers them in a
Hive Metastore, queries them with a distributed [PrestoDB](https://prestodb.io/) cluster, and
orchestrates the whole thing with [Apache Airflow](https://airflow.apache.org/) behind a
SQL-based data-quality gate.

<p align="center">
  <img src="images/lakehouse_architecture.png" alt="Lakehouse Architecture Diagram" width="100%" />
</p>

### Scope and honest limitations

This is a **learning and demonstration platform**, not a production deployment. Specifically:

- Airflow runs the `SequentialExecutor` against a **SQLite** database — one task at a time, no parallelism.
- Spark runs in **local mode** (`--master local[4]`) inside a single container. There is no Spark cluster,
  and therefore no executors — only a driver JVM.
- Every credential defaults to a well-known value (`admin`/`admin`, `minioadmin`/`minioadmin`).
- There is no authentication, TLS, or authorisation on any endpoint.
- Ingestion is **batch only**. There is no streaming or CDC path.
- The Avro schema is **contract-driven** — see
  [Schema management & evolution](#schema-management--evolution) for details on schema handling.

Everything else below is verified against the running stack.

---

## Table of contents

- [Technology stack](#technology-stack)
- [What is Apache Hudi?](#what-is-apache-hudi)
- [How the pipeline works](#how-the-pipeline-works)
- [Repository layout](#repository-layout)
- [File-by-file reference](#file-by-file-reference)
- [Service-by-service reference](#service-by-service-reference)
- [Persistent volumes](#persistent-volumes)
- [Environment variables](#environment-variables)
- [Schema management & evolution](#schema-management--evolution)
- [Quickstart](#quickstart)
- [Operating the platform](#operating-the-platform)
- [Design decisions explained](#design-decisions-explained)
- [Troubleshooting](#troubleshooting)

---

## Technology stack

| Layer | Technology | Version | Role |
| :--- | :--- | :--- | :--- |
| **Object storage** | [MinIO](https://min.io/) | `latest` | S3-compatible object store, run locally. Holds both the raw `.json.gz` staging area and the Hudi table files. Swappable for Backblaze B2 or AWS S3 via `.env` alone. |
| **Table format** | [Apache Hudi](https://hudi.apache.org/) | `0.15.0` | Adds ACID transactions, a commit timeline, record-level upserts/deletes, and inline clustering on top of Parquet files. Table type here is **Copy-on-Write**. |
| **Ingestion engine** | [Apache Spark](https://spark.apache.org/) | `3.5.0` | Hosts Hudi's `HoodieStreamer` utility, which reads JSON from object storage and writes Hudi tables. Runs in local mode. |
| **Metadata catalog** | [Apache Hive Metastore](https://hive.apache.org/) | `3.1.3` | Thrift metadata service backed by MySQL. Hudi syncs table and partition definitions here so Presto can discover them. |
| **Metastore database** | [MySQL](https://www.mysql.com/) | `8.0` | Relational backing store for the metastore's own schema (74 tables). |
| **Query engine** | [PrestoDB](https://prestodb.io/) | `0.299` | Distributed MPP SQL engine — one coordinator plus two workers, on OpenJDK 17. Reads Hudi tables through the metastore. **Read-only** for this connector. |
| **Orchestrator** | [Apache Airflow](https://airflow.apache.org/) | `2.7.2` (Python 3.9) | Schedules extraction, triggers the Spark job, runs the data-quality gate, and fires Slack alerts on failure. |
| **Visualisation** | [Apache Superset](https://superset.apache.org/) | `6.1.0` | Optional BI layer. Connects to Presto over the `pyhive` SQLAlchemy dialect. |

---

## What is Apache Hudi?

[Apache Hudi](https://hudi.apache.org/) (pronounced *"hoodie"*, originally created at Uber and an acronym for **H**adoop **U**pserts **D**eletes and **I**ncrementals) is an open-source **transactional data lakehouse table format**. It manages how data files (specifically [Apache Parquet](https://parquet.apache.org/)) are stored, mutated, and cataloged on distributed object storage such as AWS S3 or MinIO.

### The problem Hudi solves

In traditional data lakes, cloud storage stores raw, immutable flat files. That creates fundamental data engineering challenges:

- **No row-level updates or deletes:** Updating or deleting a single record requires reading, rewriting, and replacing an entire partition directory or table.
- **No transactional guarantees (ACID):** Concurrent writes risk corrupting readers, and failed pipeline runs leave uncommitted, orphaned files scattered across storage.
- **Costly privacy compliance:** Fulfilling GDPR/CCPA "Right to be Forgotten" requests requires expensive full-table rewrites.
- **The small-file problem:** Frequent batch or streaming ingestion creates millions of tiny Parquet files that overwhelm object store metadata and degrade SQL query performance.

Hudi bridges the gap between traditional data warehouses (fast updates, ACID guarantees) and cloud data lakes (cheap, scalable, open file formats) to create a **Lakehouse**.

### Core concepts in this project

| Concept | What it means | How this repository uses it |
| :--- | :--- | :--- |
| **ACID Transactions** | Atomic commits; writes succeed completely or rollback cleanly with zero reader interference. | Every batch ingested by `HoodieStreamer` commits atomically to MinIO. |
| **Record Key (`recordkey.field`)** | A unique primary key per record. | Mapped to `id` (the GitHub event ID). |
| **Precombine Key (`precombine.field`)** | An ordering field used to resolve conflicts when duplicate primary keys arrive. | Mapped to `created_at`. If two events share the same `id`, the record with the newer timestamp automatically wins. |
| **Commit Timeline (`.hoodie/`)** | An immutable transaction log tracking all commits, cleans, compactions, and rollbacks. | Stored alongside Parquet files in MinIO. Powers **time travel** and snapshot isolation for Presto queries. |
| **Table Type: Copy-on-Write (COW)** | Updates rewrite only the specific Parquet files containing modified rows. | Zero merge overhead for readers — Presto queries run against pure columnar Parquet at maximum speed. |
| **Hive-Style Partitioning** | Directories formatted as `key=value/`. | Events partition into `type=PushEvent/`, `type=PullRequestEvent/`, etc., allowing Presto to prune unused partitions instantly. |
| **Inline Clustering & Cleaning** | Automatically combines small files into optimal chunks and purges obsolete file slices. | Keeps 5 commits of history for time travel and clusters files every 4 commits without running background daemons. |

---

## How the pipeline works

<p align="center">
  <img src="images/pipeline_flow.png" alt="Pipeline Flow Diagram" width="100%" />
</p>

Task dependency is strictly linear: `download → ingest → quality gate`. A failure at any step
stops the run and triggers the Slack callback.

---

## Repository layout

```text
presto-hudi-cos/
├── README.md                             # This document
├── .env.example                          # Template for the 11 required environment variables
├── .env                                  # Your real values (git-ignored)
├── .gitignore                            # Excludes secrets, JARs (except the JDBC driver), caches, OS cruft
├── docker-compose.yml                    # Defines all 10 services, 6 volumes, and 1 network
│
├── images/
│   ├── lakehouse_architecture.png        # Architecture diagram used above
│   └── pipeline_flow.png                 # Pipeline flow diagram
│
├── airflow/
│   └── dags/
│       └── github_ingestion_dag.py       # The only DAG: 3 tasks + failure-alert callback
│
├── metastore/
│   ├── metastore-site.xml                # hive-site.xml TEMPLATE (contains {{PLACEHOLDER}} tokens)
│   └── lib/
│       └── mysql-connector-j-8.0.33.jar  # MySQL JDBC driver — the Hive image ships none
│
├── presto/
│   ├── coordinator/
│   │   ├── config.properties             # coordinator=true, discovery server, cluster memory cap
│   │   ├── jvm.config                    # -Xmx3G + 17 JDK-17 --add-opens flags
│   │   ├── node.properties               # node.environment / node.id / node.data-dir
│   │   └── catalog/hudi.properties       # Hudi connector + metastore URI
│   ├── worker-1/                         # Same four files; coordinator=false
│   └── worker-2/                         # Byte-identical to worker-1 except node.id
│
└── spark/
    ├── spark-defaults.conf               # Kryo, Hive catalog, S3A, event logging
    ├── github-ingest.properties          # 18 HoodieStreamer / Hudi table properties
    ├── github-schema.avsc                # Avro schema: 8 top-level fields, 15-field payload record
    └── hudi_acid_superpowers_demo.py     # Standalone PySpark demo (upsert / delete / time travel)
```

---

## File-by-file reference

### `docker-compose.yml`

Single source of truth for the stack. Defines **10 services**, **6 named volumes**, and one bridge
network (`lakehouse-network`). Three patterns are worth understanding:

**1. Startup ordering via health gates.** Services declare `depends_on: { condition: service_healthy }`
rather than plain `depends_on`, so Hive waits for MySQL to actually accept connections, Presto waits
for Hive, and Airflow waits for `minio-init` to have *completed successfully*
(`condition: service_completed_successfully`).

**2. Config templating at container start.** Several images need configuration that depends on `.env`
values, which static config files cannot express. Each affected service rewrites its config in its
entrypoint before starting the real process — see the service reference below for the exact steps.

**3. A YAML anchor for Presto.** `x-presto-entrypoint: &presto-entrypoint` is defined once at the top
of the file and referenced by all three Presto services via `*presto-entrypoint`, so the coordinator
and both workers share one entrypoint definition.

### `airflow/dags/github_ingestion_dag.py`

The complete pipeline — one DAG (`github_events_ingestion`, `schedule='@daily'`, `catchup=False`)
with three `PythonOperator` tasks and one callback. Default args set `retries: 1` and
`retry_delay: 5 minutes`.

#### `download_and_upload_github_data()` → task `download_and_upload_raw_data`

Stages raw data into object storage using **boto3** (the S3 API, not Hadoop's `s3a://`).

- Computes `base_time = utcnow() - 3 hours`, then walks **24 hours backwards** from there. The
  3-hour offset exists because GitHub Archive publishes each hourly file with a lag; requesting the
  current hour would reliably 404. Effective window: roughly `now-3h` back to `now-27h`.
- Builds filenames as `YYYY-MM-DD-H.json.gz`. Note the hour is **not zero-padded** — that is
  GitHub Archive's actual naming convention (`2026-09-08-9.json.gz`, not `-09`).
- Deletes everything under the `github-raw/` prefix first, so each run ingests a clean, deterministic batch.
- Downloads each file with up to **3 attempts** and linear backoff, rejects any response under
  1 KB as truncated, uploads to `s3://$S3_BUCKET_NAME/github-raw/`, and deletes the local temp file
  in a `finally` block.
- Uses `Config(s3={'addressing_style': 'path'})`, required for MinIO, which does not support
  virtual-host-style bucket addressing.
- **Tolerates partial failure**: it aborts only if *all 24* files fail. Any lesser number logs a
  warning and continues, because the newest hour is often not yet published.

#### `trigger_spark_ingestion()` → task `trigger_hudi_ingestion`

Runs the Spark job **in a different container**. Airflow mounts `/var/run/docker.sock`, so it uses
the Docker SDK (`docker.from_env()`) to `exec` `spark-submit` inside `spark-client`.

Key submit arguments:

| Argument | Value | Why |
| :--- | :--- | :--- |
| `--master` | `local[4]` | Caps concurrent partitions at 4. See [Design decisions](#why-local4-and-6-gb-of-driver-memory). |
| `--driver-memory` | `6G` | In local mode the driver does all the work; there are no executors. |
| `--class` | `org.apache.hudi.utilities.streamer.HoodieStreamer` | Hudi's batch/streaming ingestion utility. |
| `--packages` | `hudi-utilities-slim-bundle_2.12:0.15.0`, `hudi-spark3.5-bundle_2.12:0.15.0`, `hadoop-aws:3.3.4` | The slim bundle must be paired with the matching Spark bundle. |
| `--source-class` | `JsonDFSSource` | Reads newline-delimited JSON from a filesystem/object-store path. |
| `--source-ordering-field` | `created_at` | Hudi's precombine key — on duplicate record keys, the later value wins. |
| `--table-type` | `COPY_ON_WRITE` | Rewrites Parquet files on update. Best read performance; higher write cost. |
| `--source-limit` | `1073741824` | Caps one batch at 1 GiB of source bytes. A full day is ~430 MB, so it does not normally bind. |
| `--enable-hive-sync` | — | Registers the table and its partitions in the Hive Metastore. |

After the job, the task performs **two** checks:

1. Non-zero exit code → `RuntimeError` with the last 2000 characters of output.
2. Output containing `"Nothing to commit"` → `RuntimeError`. This is a deliberate **circuit
   breaker**: Hudi exits 0 when it finds no new source files, which would otherwise mark the task
   green without ingesting a single row.

#### `run_data_quality_assertions()` → task `run_data_quality_assertions`

Connects to Presto over PyHive's DB-API (`pyhive.presto`) and runs six queries. It first reads
`max(_hoodie_commit_time)` to identify the newest commit instant, then **scopes all five gates to
that instant** so each gate judges only the batch that was just written, not the table's history:

| Gate | Assertion | Fails when |
| :--- | :--- | :--- |
| 1 | Batch volume | The newest commit contains 0 rows |
| 2 | Primary-key integrity | Any row has `id IS NULL OR trim(id) = ''` |
| 3 | Deduplication | `count(id) - count(DISTINCT id) > 0` |
| 4 | Freshness | `max(created_at)` is more than 24 hours old |
| 5 | Partition spread | Fewer than 2 distinct `type` values |

The whole body sits inside `try ... finally`, which closes the cursor and connection independently
so a failure in one cannot leak the other.

#### `pipeline_failure_alert(context)`

`on_failure_callback` for every task. Prints a failure summary to the task log and, when
`SLACK_WEBHOOK_URL` is set, POSTs a formatted message to it via `urllib.request` (10-second
timeout). Webhook errors are caught and logged — alerting can never itself fail the DAG.

### `metastore/metastore-site.xml`

A **template**, not a finished config. It is mounted read-only at
`/opt/hive/conf/hive-site.xml.template`, and the metastore entrypoint copies it to `hive-site.xml`
after substituting three placeholder tokens: `{{S3_ENDPOINT}}`, `{{S3_PATH_STYLE_ACCESS}}`, and
`{{S3_SSL_ENABLED}}`.

Contents:

| Property | Purpose |
| :--- | :--- |
| `javax.jdo.option.ConnectionURL` | `jdbc:mysql://mysql-db:3306/metastore_db` with `createDatabaseIfNotExist=true` |
| `javax.jdo.option.ConnectionDriverName` | `com.mysql.cj.jdbc.Driver` |
| `javax.jdo.option.ConnectionUserName` | `hive` |
| `hive.metastore.uris` | `thrift://hive-metastore:9083` |
| `hive.metastore.schema.verification` | `false` |
| `datanucleus.schema.autoCreateAll` | `false` — schema creation is handled by `schematool`, not DataNucleus |
| `fs.s3a.endpoint` | Templated from `S3_ENDPOINT` |
| `fs.s3a.path.style.access` | Templated from `S3_PATH_STYLE_ACCESS` |
| `fs.s3a.connection.ssl.enabled` | Templated from `S3_SSL_ENABLED` |
| `fs.s3a.aws.credentials.provider` | `EnvironmentVariableCredentialsProvider` |

Two things this file deliberately does **not** contain:

- **The database password.** It is injected as a JVM system property via `HADOOP_OPTS`
  (`-Djavax.jdo.option.ConnectionPassword=$MYSQL_PASSWORD`), so the credential lives only in `.env`.
  This works because Hive's `HiveConf` explicitly copies matching JVM system properties over its XML.
- **AWS keys.** Only the *provider class* is named; the actual keys are read from the
  `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables at runtime.

> **Why S3A config must live in this file rather than in `HADOOP_OPTS`:** Hadoop's `Configuration`
> class does **not** read JVM system properties as config keys — unlike `HiveConf`, which does. A
> `-Dfs.s3a.endpoint=…` flag is silently ignored by S3A. That is why the endpoint is templated into
> XML, and why the entrypoint additionally symlinks this file as Hadoop's `core-site.xml`.

### `metastore/lib/mysql-connector-j-8.0.33.jar`

The MySQL JDBC driver (2.4 MB), mounted into `/opt/hive/lib/`. The `apache/hive:3.1.3` image ships
**no** MySQL driver, so without this the metastore cannot reach its own database. This is the one
JAR deliberately committed to the repository — `.gitignore` excludes `*.jar` but re-includes it with
`!metastore/lib/*.jar`.

### `presto/*/config.properties`

Node role and cluster topology.

**Coordinator:**

```properties
coordinator=true
node-scheduler.include-coordinator=false   # coordinator schedules but does not execute
http-server.http.port=8080
query.max-memory=1.5GB                     # cluster-wide cap, enforced by the coordinator
query.max-memory-per-node=1GB              # see note below
query.max-total-memory-per-node=1.5GB      # see note below
discovery-server.enabled=true              # embedded discovery service
discovery.uri=http://presto-server:8080
```

**Workers** set `coordinator=false`, omit the discovery server and `query.max-memory`, and point
`discovery.uri` at the coordinator.

> **Note:** because `node-scheduler.include-coordinator=false`, the coordinator never runs query
> tasks, so its own `query.max-memory-per-node` and `query.max-total-memory-per-node` have no
> practical effect. Presto still *binds* them (it refuses to start on genuinely unrecognised
> properties), so they are harmless — just inert. The values that matter are `query.max-memory`
> on the coordinator and the per-node limits on the workers.

### `presto/*/jvm.config`

```properties
-Xmx3G                     # heap ceiling
-XX:+UseG1GC
-XX:G1ReservePercent=15
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
-Djdk.attach.allowAttachSelf=true
+ 17 × --add-opens=…       # JDK 17 module access
```

The **17 `--add-opens` flags are load-bearing and easy to lose.** The `prestodb/presto:0.299` image
ships its own `jvm.config` containing them, but this repository bind-mounts over that path — so
omitting them silently strips them. Presto still starts without them, then throws
`InaccessibleObjectException` deep inside a query when JDK 17's strong encapsulation blocks
reflective access. The list here is copied verbatim from the image default.

> **General rule for this repo:** before bind-mounting over a path an image already populates, diff
> against the image first (`docker run --rm --entrypoint cat <image> <path>`). Silent clobbering is
> the most likely source of subtle breakage here.

### `presto/*/node.properties`

Node identity. `node.environment=test` must match across all nodes or they will not join the same
cluster. `node.id` is a stable per-node identifier (optional — Presto generates and persists one if
absent). `node.data-dir=/var/presto/data` is where local state and logs go.

### `presto/*/catalog/hudi.properties`

Defines the `hudi` catalog:

```properties
connector.name=hudi
hive.metastore.uri=thrift://hive-metastore:9083
```

Only two lines, because everything S3-related is appended at container start from `.env`. All three
copies are byte-identical.

> **The Presto Hudi connector is read-only.** `CREATE TABLE`, `CREATE TABLE AS`, `INSERT`, and
> `DROP TABLE` all fail with *"This connector does not support …"*. Use Spark SQL for any DDL or DML
> against `hudi.default.github_events`.

### `spark/spark-defaults.conf`

Baseline Spark configuration, copied into the writable `SPARK_CONF_DIR` at startup and then extended
with the three `.env`-derived S3A values.

| Property | Value | Purpose |
| :--- | :--- | :--- |
| `spark.serializer` | `KryoSerializer` | Required by Hudi. |
| `spark.sql.catalogImplementation` | `hive` | Use the Hive Metastore as the catalog. |
| `spark.sql.hive.convertMetastoreParquet` | `false` | **Essential.** Forces Spark to use Hudi's input format instead of its own Parquet reader, which would ignore the Hudi timeline and return duplicate/stale rows. |
| `spark.hadoop.hive.metastore.uris` | `thrift://hive-metastore:9083` | Metastore location. |
| `spark.hadoop.fs.s3a.impl` | `S3AFileSystem` | Explicit S3A binding. |
| `spark.hadoop.fs.s3a.aws.credentials.provider` | `EnvironmentVariableCredentialsProvider` | Read keys from the environment. |
| `spark.jars.packages` | `hudi-spark3.5-bundle`, `hadoop-aws` | Auto-loaded for interactive sessions such as the demo script. |
| `spark.eventLog.enabled` / `.dir` | `true` / `file:///tmp/spark-events` | Write event logs for the History Server. |
| `spark.history.fs.logDirectory` | `file:///tmp/spark-events` | Where the History Server reads them from. |

### `spark/github-ingest.properties`

18 Hudi properties passed to `HoodieStreamer` via `--props`.

| Group | Properties | Meaning |
| :--- | :--- | :--- |
| **Table keys** | `recordkey.field=id`, `precombine.field=created_at`, `partitionpath.field=type` | `id` is the primary key; on conflict the later `created_at` wins; files are partitioned by event type. |
| **Partition style** | `hive_style_partitioning=true` | Directories are written as `type=PushEvent/`, which Hive and Presto understand natively. |
| **Metastore sync** | `hive_sync.enable`, `.mode=hms`, `.database=default`, `.table=github_events`, `.metastore.uris` | Sync straight to the metastore over Thrift (`hms` mode), not via a HiveServer2 JDBC connection. |
| **Schema provider** | `schemaprovider.class=FilebasedSchemaProvider`, `source.schema.file`, `target.schema.file` | Both source and target schemas come from `github-schema.avsc`. |
| **Lifecycle** | `metadata.enable=false`, `cleaner.commits.retained=5`, `clustering.inline=true`, `clustering.inline.max.commits=4` | Metadata table off (simpler for a demo); keep 5 commits of history for time travel; compact small files inline every 4 commits. |

### `spark/github-schema.avsc`

The Avro schema that defines the table. **8 top-level fields**, matching GitHub Archive's event
envelope exactly:

| Field | Type | Notes |
| :--- | :--- | :--- |
| `id` | `string` | Hudi record key. GitHub returns this as a string, not a number. |
| `type` | `string` | Event type — also the partition column. |
| `public` | `boolean` | |
| `created_at` | `string` | ISO-8601 (`2026-09-08T10:00:00Z`). Hudi precombine field. |
| `actor` | record (6) | `id`, `login`, `display_login`, `gravatar_id`, `url`, `avatar_url` |
| `repo` | record (3) | `id`, `name`, `url` |
| `org` | record (5) | `id`, `login`, `gravatar_id`, `url`, `avatar_url` — present on ~14% of events |
| `payload` | record (15) | See below |

The `payload` record holds 11 scalars — `action`, `ref`, `ref_type`, `master_branch`,
`description`, `pusher_type`, `push_id`, `head`, `before`, `number`, `repository_id` — plus four
nested records: `issue`, `pull_request` (including `head`/`base` sub-records carrying branch `ref`
and commit `sha`), `comment`, and `release`.

Every field is a nullable union (`["null", T]`) with `default: null`, because GitHub's payload shape
varies by event type: a `PushEvent` populates `push_id`/`ref`/`head`/`before` while a
`PullRequestEvent` populates `action`/`number`/`pull_request` instead.

### `spark/hudi_acid_superpowers_demo.py`

A **standalone, manually-run** PySpark script (nothing in the DAG or compose file invokes it; it is
mounted into `spark-client` ready to use). It requires the table to exist and exits with a clear
message if it does not. Three demonstrations:

1. **ACID upsert** — picks one `PullRequestEvent` or `IssuesEvent`, rewrites its `created_at` to the
   current UTC timestamp, and writes with `operation=upsert`. The row count is unchanged afterwards,
   showing record-level mutation without duplication or a full table rewrite.
2. **GDPR point delete** — selects one `actor.login`, then writes that user's record keys with
   `operation=delete`, demonstrating "right to be forgotten" without rewriting the table.
3. **Time travel** — lists the distinct `_hoodie_commit_time` values from the Hudi timeline.

### `.env.example` / `.env`

`.env.example` is the checked-in template; copy it to `.env` (git-ignored) before first launch. Both
files carry the same **11 keys** — see [Environment variables](#environment-variables).

### `.gitignore`

Excludes `.env`, `__pycache__/`, `*.pyc`, IDE directories, `.DS_Store`, and `*.jar` — with an
explicit `!metastore/lib/*.jar` exception so the required MySQL JDBC driver is still tracked.

---

## Service-by-service reference

### `minio` — object storage

Runs `server /data --console-address ":9001"`. Root credentials come from `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY`, so one credential pair works for MinIO, boto3, Spark's S3A, and Presto
alike. Health-gated on `/minio/health/live`, which every dependent service waits for.

### `minio-init` — one-shot bucket creation

A short-lived container using the same MinIO image for its bundled `mc` client. It registers an
alias, runs `mc mb --ignore-existing` for `$S3_BUCKET_NAME`, prints a confirmation, and exits 0.
Airflow depends on it with `condition: service_completed_successfully`, so the DAG can never start
before the bucket exists. Seeing this container in `Exited (0)` state is **correct**, not a failure.

### `mysql-db` — metastore backing database

MySQL 8.0 with a `metastore_db` database and a `hive` user. Its health check uses `CMD-SHELL` (not
`CMD`) specifically so `$MYSQL_PASSWORD` is expanded by a shell — with the plain `CMD` form the
variable is passed as a literal string.

### `hive-metastore` — metadata catalog

The most involved entrypoint in the stack. In order:

1. Copy `hive-site.xml.template` → `hive-site.xml`.
2. `sed` the three `{{…}}` S3 placeholders with live `.env` values.
3. **Symlink** `hive-site.xml` to `/opt/hadoop/etc/hadoop/core-site.xml`, so Hadoop's S3A client
   reads the same endpoint settings. (The image's own `core-site.xml` is an empty
   `<configuration/>`, so nothing is lost.)
4. Run `schematool -dbType mysql -info || schematool -dbType mysql -initSchema` — an
   **idempotent self-heal**: probe for an existing schema, and initialise the 74 metastore tables
   only if the probe fails. Safe on both a fresh volume and a restart.
5. `IS_RESUME=true exec /entrypoint.sh` — hands off to the image's own entrypoint with schema
   initialisation disabled, since step 4 already handled it.

> Step 4 exists because setting `IS_RESUME=true` alone means the image **never** creates the schema.
> On a fresh volume the metastore would start, pass its TCP health check, and then fail every
> operation with `Table 'metastore_db.DBS' doesn't exist`.

Runs as `root` because steps 1–3 write inside `/opt`.

### `presto-server`, `presto-worker-1`, `presto-worker-2` — query cluster

All three share one entrypoint (a YAML anchor). Because `/opt/presto-server/etc/` contains read-only
bind mounts, the entrypoint **copies the whole directory to `/tmp/etc`**, appends five
`hive.s3.*` lines derived from `.env` to the catalog file there, and launches with
`--etc-dir /tmp/etc`. This is what keeps credentials out of the repository while leaving the mounted
files untouched.

The coordinator's health check polls `/v1/info` for `"starting":false`; both workers gate on it.

### `spark-client` — ingestion runtime

A long-lived idle container (`tail -f /dev/null`) that exists to be `docker exec`-ed into by Airflow.
Its entrypoint:

1. Creates `/tmp/spark-conf` and `/tmp/spark-events`.
2. Copies `spark-defaults.conf` into `/tmp/spark-conf` (pointed to by `SPARK_CONF_DIR`) and appends
   the three `.env`-derived S3A properties.
3. Starts the **Spark History Server** on port 18080.
4. **Pre-fetches Hudi dependencies in the background** if
   `/root/.ivy2/jars/…hudi-utilities-slim-bundle…jar` is absent, by running a throwaway
   `spark-submit --packages … --help`. This warms the Ivy cache so the first real ingestion does not
   pay a multi-minute dependency resolution cost.
5. Idles.

### `airflow` — orchestrator

Runs `airflow standalone` (scheduler + webserver in one process) with the `SequentialExecutor` over
SQLite. Its entrypoint installs `docker`, `boto3`, and `pyhive[presto]` **only if they are not
already importable**, so restarts are fast. It also appends `SESSION_COOKIE_NAME = 'airflow_session'`
to `webserver_config.py`, which prevents session-cookie collisions with Superset when both are open
on `localhost` in the same browser.

Mounts `/var/run/docker.sock` so it can drive `spark-client`. This is effectively root on the host —
acceptable for local development, unacceptable for a shared environment.

### `superset` — visualisation (optional)

On first boot installs `pyhive` and `presto-python-client` into its virtualenv, runs
`superset db upgrade`, creates the admin user, runs `superset init`, and drops a `.initialized`
marker into its volume so subsequent starts skip all of it. Nothing in the pipeline depends on
Superset; it can be removed if you do not need dashboards.

---

## Persistent volumes

| Volume | Mounted at | Contents | Effect of `docker compose down -v` |
| :--- | :--- | :--- | :--- |
| `minio-data` | `minio:/data` | **All raw files and Hudi tables** | Total data loss |
| `mysql-data` | `mysql-db:/var/lib/mysql` | Metastore schema, table/partition registry | Table definitions lost; `schematool` recreates the schema on next boot |
| `airflow-data` | `airflow:/opt/airflow/data` | `airflow.db` — DAG run history, task states, users | Run history lost |
| `airflow-logs` | `airflow:/opt/airflow/logs` | Per-task log files | Logs lost |
| `spark-ivy-cache` | `spark-client:/root/.ivy2` | ~149 resolved Hudi/Hadoop JARs | Next ingestion re-downloads dependencies |
| `superset-data` | `superset:/app/superset_home` | Superset metadata DB, saved charts | Dashboards lost |

Note that `airflow-data` is mounted at `/opt/airflow/data`, a **subdirectory**, and the database URL
points at `/opt/airflow/data/airflow.db`. Mounting a volume at `/opt/airflow` itself would shadow
both the `dags/` bind mount and Airflow's own installation.

---

## Environment variables

Copy `.env.example` to `.env`. All 11 keys have working defaults for local MinIO.

| Variable | Default | Consumed by |
| :--- | :--- | :--- |
| `AWS_ACCESS_KEY_ID` | `minioadmin` | MinIO root user; boto3, S3A, and Presto S3 credentials |
| `AWS_SECRET_ACCESS_KEY` | `minioadmin` | As above |
| `AWS_REGION` | `us-east-1` | boto3 only (Airflow). MinIO ignores it. |
| `S3_ENDPOINT` | `http://minio:9000` | Airflow, Spark, Presto, Hive |
| `S3_BUCKET_NAME` | `github-raw-data` | `minio-init`, Airflow, Spark, the demo script |
| `S3_PATH_STYLE_ACCESS` | `true` | Spark, Presto, Hive — **must** be `true` for MinIO |
| `S3_SSL_ENABLED` | `false` | Spark, Presto, Hive — `false` for plain-HTTP MinIO |
| `MYSQL_ROOT_PASSWORD` | `rootpassword` | MySQL container |
| `MYSQL_PASSWORD` | `hivepassword` | MySQL container; injected into Hive via `HADOOP_OPTS` |
| `SUPERSET_SECRET_KEY` | *(long default)* | Superset session signing |
| `SLACK_WEBHOOK_URL` | *(placeholder)* | Airflow failure alerts — leave unset to disable |

### Switching to Backblaze B2 or AWS S3

No code changes are needed; the S3 layer is fully `.env`-driven:

```bash
AWS_ACCESS_KEY_ID=<your key>
AWS_SECRET_ACCESS_KEY=<your secret>
AWS_REGION=us-east-005
S3_ENDPOINT=s3.us-east-005.backblazeb2.com   # no scheme → https is assumed
S3_BUCKET_NAME=<your bucket>
S3_PATH_STYLE_ACCESS=false                   # cloud providers use virtual-host addressing
S3_SSL_ENABLED=true
```

Then stop the `minio` and `minio-init` services — the bucket must already exist remotely.

---

## Schema management & evolution

This lakehouse enforces a **contract-driven schema** using Apache Avro (`spark/github-schema.avsc`) and Hudi's `FilebasedSchemaProvider`.

---

### How schema enforcement works

1. **Explicit Schema Contract:** Every incoming JSON event from GitHub Archive is mapped onto `github-schema.avsc`. Any raw JSON fields not defined in this Avro schema are safely filtered out during ingestion.
2. **Table-Level Evolution:** When new columns are added to the Avro schema, Hudi's `hoodie.schema.on.read.enable=true` and `hoodie.datasource.write.reconcile.schema=true` automatically reconcile incoming batches against historical commits. Older files return `NULL` for newly added fields without requiring table rewrites.

---

### Schema change compatibility matrix

| Change Type | Example | In-Place Evolution? | Action Required |
| :--- | :--- | :---: | :--- |
| **Add nullable field** | Adding a new field to `payload` (e.g. `reactions`) | **Yes (Safe)** | Add the field to `github-schema.avsc` with `default: null`. Next DAG run applies it automatically. |
| **Widen data type** | Promoting `int` to `long` | **Yes (Safe)** | Update type in `github-schema.avsc`. Hudi reconciles the promotion on read. |
| **Incompatible change** | Renaming/deleting fields, changing `struct` to `string` | **No (Breaking)** | Hive Metastore rejects incompatible types to protect catalog integrity. Requires a table recreation. |

---

### Applying breaking (incompatible) schema changes

If you make a breaking schema change (such as altering nested struct hierarchies or removing existing columns), recreate the table definition using this 3-step workflow:

#### Step 1: Drop the catalog registration in Spark SQL
The table is registered as `EXTERNAL`, so dropping it removes only the Hive catalog entry — raw data files remain safe:
```bash
docker exec spark-client /opt/spark/bin/spark-sql \
  --master "local[2]" --driver-memory 1G \
  -e "DROP TABLE IF EXISTS default.github_events;"
```
*(Use `spark-sql` rather than `presto-cli`, as the Presto Hudi connector is read-only).*

#### Step 2: Delete existing Hudi storage files
In the **[MinIO Console](http://localhost:9001)** (`minioadmin` / `minioadmin`), navigate to the `github-raw-data` bucket and delete the `hudi-tables/` prefix. This purges old Parquet files and resets Hudi's commit checkpoint.

#### Step 3: Re-trigger the ingestion DAG
Trigger the pipeline via the [Airflow UI](http://localhost:8085) or run:
```bash
docker exec airflow airflow dags trigger github_events_ingestion
```
Spark will ingest the raw staging data under your new schema and register fresh table metadata in the Hive Metastore.

---

## Quickstart

Get the entire lakehouse platform up and running in under 5 minutes.

---

### Prerequisites

* **Docker & Docker Compose:** Docker Engine 24.0+ and Docker Compose 2.20+
* **Memory Allocation:** At least **16 GB RAM** allocated to Docker (Docker Desktop → Settings → Resources → Memory)
* **Disk Space:** ~10 GB free disk space for images and local data

---

### 1. Configure environment

Copy the pre-configured environment template:

```bash
cp .env.example .env
```
*(Default values work out-of-the-box for local MinIO object storage. Optionally set `SLACK_WEBHOOK_URL` for alerts).*

---

### 2. Start the platform

Launch all 10 services in the background:

```bash
docker compose up -d
```

> **Note:** Initial startup downloads container images (~6 GB) and pre-warms Hudi's dependency cache. Allow 2–3 minutes for all containers to reach a healthy state.

---

### 3. Verify & access web interfaces

Confirm container health:

```bash
docker compose ps
```
*(All core services will report `healthy` or `running`. The helper `minio-init` container will show `Exited (0)`, which is expected).*

Once services are healthy, access the web interfaces directly in your browser:

| Service / Interface | URL | Credentials | Purpose |
| :--- | :--- | :--- | :--- |
| **Airflow Webserver** | [http://localhost:8085](http://localhost:8085) | `admin` / `admin` | Trigger and monitor ingestion DAG runs |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `minioadmin` / `minioadmin` | Inspect raw staging bucket and Hudi table files |
| **Apache Superset** | [http://localhost:8088](http://localhost:8088) | `admin` / `admin` | SQL Lab queries, metrics, and BI dashboards |
| **Spark History Server** | [http://localhost:18080](http://localhost:18080) | *(No auth)* | Review completed Spark `HoodieStreamer` job runs |
| **Presto Coordinator** | [http://localhost:8080](http://localhost:8080) | *(No auth)* | Cluster status, active queries, and worker nodes |

👉 Next step: Proceed to **[Operating the platform](#operating-the-platform)** to trigger your first ingestion run.

---

## Operating the platform

A complete step-by-step workflow: from triggering ingestion and running interactive SQL queries, to demonstrating Hudi's ACID superpowers and building dashboards.

---

### 1. Ingest data via Airflow

Trigger the automated 3-task pipeline (`download → ingest → quality gate`):

```bash
docker exec airflow airflow dags unpause github_events_ingestion
docker exec airflow airflow dags trigger github_events_ingestion
```

* **Web UI:** Monitor execution in real time at [http://localhost:8085](http://localhost:8085) (`admin` / `admin`).
* **Spark Live Progress:** While Task 2 is running, watch executors and stages at [http://localhost:4040](http://localhost:4040).
* **Spark History:** Review completed job metrics afterwards at [http://localhost:18080](http://localhost:18080).

> **Runtime note:** A full 24-hour batch processes ~1.5 million GitHub events across 16 event-type partitions. Downloading raw archives from GitHub dominates the runtime.

---

### 2. Query the lakehouse with Presto

Launch the interactive Presto CLI connected to the Hudi catalog:

```bash
docker exec -it presto-server presto-cli --catalog hudi --schema default
```

#### A. Discover tables & partition distribution
```sql
SHOW TABLES;

-- Event volume by partition (demonstrating partition pruning)
SELECT type, count(*) AS events
FROM github_events
GROUP BY type
ORDER BY events DESC;
```

#### B. Query deep nested JSON / Avro structures
Because the table is backed by a strongly-typed Avro schema, nested attributes query with standard SQL dot-notation:

```sql
-- Extract git commit details from push events
SELECT payload.push_id, payload.ref, payload.head
FROM github_events
WHERE type = 'PushEvent'
LIMIT 5;

-- Extract source and target branches from pull requests
SELECT payload.pull_request.number,
       payload.pull_request.head.ref AS source_branch,
       payload.pull_request.base.ref AS target_branch
FROM github_events
WHERE type = 'PullRequestEvent'
LIMIT 5;
```

#### C. Inspect Hudi's ACID commit timeline
Query Hudi's internal metadata column `_hoodie_commit_time` to inspect historical write batches:

```sql
SELECT _hoodie_commit_time, count(*) AS rows
FROM github_events
GROUP BY _hoodie_commit_time
ORDER BY _hoodie_commit_time DESC;
```

> **Note:** The Presto Hudi connector is **read-only** (`SELECT` queries only). DDL/DML write operations (`INSERT`, `UPDATE`, `DROP`) are managed via Apache Spark.

---

### 3. Run the Hudi ACID superpowers demo

Run the bundled PySpark demonstration script to witness Hudi's lakehouse capabilities in action:

```bash
docker exec -it spark-client /opt/spark/bin/spark-submit \
  --master "local[4]" --driver-memory 2G \
  /opt/spark/hudi_acid_superpowers_demo.py
```

*(Prerequisite: Requires at least one completed ingestion DAG run).*

This interactive script executes three live demonstrations:
1. ⚡ **ACID In-Place Upsert:** Selects an existing Issue or PR event and updates its timestamp. The record mutates in-place on storage without creating duplicates or requiring a full table rewrite.
2. 🔒 **GDPR Point Delete ("Right to be Forgotten"):** Selects an active user login and permanently purges all associated activity records by primary key without rewriting unaffected files.
3. 🕒 **Time Travel Inspection:** Reads Hudi's `.hoodie/` timeline to list active commit instants and display data states across historical points in time.

---

### 4. Visualize data in Apache Superset

Connect Superset to Presto to create charts and dashboards:

1. Open **[http://localhost:8088](http://localhost:8088)** in your browser (`admin` / `admin`).
2. Navigate to **Settings** (top right) → **Database Connections** → **+ Database**.
3. Select **Presto** from the database dropdown.
4. Enter the internal Docker SQLAlchemy URI:
   ```text
   presto://superset@presto-server:8080/hudi/default
   ```
5. Click **Test connection** (expect a green success message), then click **Connect**.
6. Open **SQL Lab → SQL Editor**, select the `hudi` catalog and `default` schema, and start querying.

---

## Design decisions explained

The engineering rationale, trade-offs, and production context behind key architectural choices.

---

### Why `local[4]` and 6 GB of driver memory?

* **The Decision:** Run Spark with `--master local[4]` and `--driver-memory 6G` rather than default `--master local[*]`.
* **The Problem:** GitHub Archive files are `.json.gz` (compressed gzip). Gzip files are **non-splittable** — each file must be decompressed as a single stream, inflating roughly **10x in RAM**. On a modern 8- or 12-core machine, default Spark (`local[*]`) attempts to decompress 8 to 12 files simultaneously in a single JVM, causing immediate `OutOfMemoryError: Java heap space`.
* **Why this works:** Capping concurrency at 4 (`local[4]`) limits simultaneous file inflations to 4 at a time, while 6 GB provides ample heap headroom for Hudi's sorting and indexing.
* **Key Takeaway:** In Spark local mode, there are no worker executors. All tasks run inside the driver JVM, meaning only `--driver-memory` has an effect (`--executor-memory` is ignored).

---

### Why Copy-on-Write rather than Merge-on-Read?

* **The Decision:** Configure Hudi as `COPY_ON_WRITE` (`--table-type COPY_ON_WRITE`).
* **The Trade-off:**
  | Table Type | Write Speed | Query / Read Speed | Ideal Workload |
  | :--- | :--- | :--- | :--- |
  | **Copy-on-Write (COW)** *(Chosen)* | Slower (rewrites Parquet files on update) | **Fastest** (pure columnar Parquet reads) | Analytical queries, BI dashboards, batch jobs |
  | **Merge-on-Read (MOR)** | Fastest (appends updates to delta Avro logs) | Slower (readers must merge base + delta files) | High-frequency streaming / real-time CDC |
* **Why this works:** This pipeline ingests in hourly/daily batches and serves interactive SQL queries to Presto and Superset. Optimizing for **blazing-fast read speed** is far more valuable than write latency. Presto queries plain Parquet files directly with zero merge overhead.

---

### Why partition by `type`?

* **The Decision:** Store events in directories partitioned by event type (`type=PushEvent/`, `type=PullRequestEvent/`).
* **Why this works:** It makes **partition pruning** easy to demonstrate in SQL. When a query includes `WHERE type = 'PushEvent'`, Presto skips all other partition folders and only reads the relevant Parquet files.
* **Production Reality:** In real-world enterprise deployments, event data is almost always partitioned by **ingestion date** (`year=.../month=.../day=...`). GitHub events are heavily skewed — `PushEvent` accounts for ~85% of all events, creating uneven partition sizes. Date partitioning guarantees evenly distributed file sizes across object storage.

---

### Why does Airflow use the Docker socket instead of a Spark operator?

* **The Decision:** Airflow mounts `/var/run/docker.sock` and triggers `spark-submit` inside the `spark-client` container via `docker exec`.
* **The Problem:** Using Airflow's built-in `SparkSubmitOperator` requires bundling Java, Spark binaries, and ~150 MB of Hudi/Hadoop JAR dependencies directly into the Airflow container image.
* **Why this works:** Keeping Spark dependencies isolated inside `spark-client` keeps Airflow lightweight, fast to boot, and free of Java/Hadoop library conflicts.
* **Production Note:** Mounting the Docker socket grants host-level container control to Airflow. This is safe and convenient for local development, but in a production cloud environment, you would use a remote API such as Apache Livy or the Kubernetes Spark Operator.

---

### Why is there a "Nothing to commit" circuit breaker?

* **The Decision:** Fail the ingestion task if Spark output contains `"Nothing to commit"`.
* **The Problem:** If no new raw files exist on MinIO (or if a previous run already consumed them), Hudi exits with code `0` (Success) without writing any new rows.
* **Why this works:** Without this check, Airflow would see exit code `0`, mark the task **green**, and proceed to validate stale data from a previous commit. The circuit breaker catches this silent false-success and raises an alert.

---

## Troubleshooting

Common issues, root causes, and step-by-step resolutions.

---

### 1. Containers & Startup

#### `minio-init` container status is `Exited (0)`
* **Explanation:** Normal behaviour. `minio-init` is a one-shot setup container that creates the `github-raw-data` bucket on MinIO, prints a confirmation, and exits cleanly.
* **Verification:** Run `docker logs minio-init` to confirm `MinIO storage initialized successfully`.
* **Technical Note:** Airflow declares `depends_on: { minio-init: { condition: service_completed_successfully } }`, so it waits for this container to exit with code `0` before starting the scheduler.

#### Metastore fails with `Table 'metastore_db.DBS' doesn't exist`
* **Cause:** Hive Metastore started before its MySQL database finished creating the catalog tables.
* **Resolution:** Re-run the metastore's schema initialization tool:
  ```bash
  docker exec hive-metastore /opt/hive/bin/schematool -dbType mysql -initSchema
  ```
* **Technical Note:** The container entrypoint executes `schematool -dbType mysql -info || schematool -dbType mysql -initSchema`. If MySQL was interrupted during first initialization, triggering `schematool` manually restores the 74 metadata tables.

---

### 2. Ingestion & Pipeline Issues

#### Presto error: `Table hudi.default.github_events does not exist`
* **Cause:** The table has not been created yet because the Airflow pipeline has not run.
* **Resolution:** Trigger the DAG via the [Airflow UI](http://localhost:8085) (`admin`/`admin`) or run:
  ```bash
  docker exec airflow airflow dags trigger github_events_ingestion
  ```
* **Technical Note:** If the DAG reported success but the table is still missing, inspect the `trigger_hudi_ingestion` task logs for `"Nothing to commit"`. This indicates no raw `.json.gz` files were found in `s3://github-raw-data/github-raw/`.

#### Spark job fails with `OutOfMemoryError: Java heap space`
* **Cause:** Spark exceeded driver heap memory while extracting and parsing large compressed GitHub event files.
* **Resolution (choose one):**
  1. **Allocate more RAM (Recommended):** In Docker Desktop → Settings → Resources, allocate at least **16 GB** memory.
  2. **Reduce concurrency:** In `airflow/dags/github_ingestion_dag.py`, change `--master local[4]` to `--master local[2]` to cut concurrent partition inflation in half.
  3. **Lower batch limit:** In `airflow/dags/github_ingestion_dag.py`, reduce `--source-limit` to `268435456` (256 MB).
* **Technical Note:** GitHub Archive `.json.gz` files are non-splittable, so each file becomes one Spark partition. Local mode runs all tasks in a single driver JVM. You can inspect actual executor memory usage at `http://localhost:18080` → application → **Environment**.

#### Airflow web UI lost previous DAG run history
* **Cause:** Running `docker compose down -v` deletes Docker storage volumes, including `airflow-data` where SQLite stores run history.
* **Resolution:** To stop containers without losing run history or ingested tables, run `docker compose down` (without the `-v` flag).

---

### 3. Querying & Storage

#### Presto error: `Hoodie table not found in path …/.hoodie`
* **Cause:** Hive Metastore remembers that the table exists, but the physical data files in MinIO storage were deleted.
* **Resolution:** Drop the orphaned catalog entry using Spark SQL, then re-run the pipeline:
  ```bash
  docker exec spark-client /opt/spark/bin/spark-sql \
    --master "local[2]" --driver-memory 1G \
    -e "DROP TABLE IF EXISTS default.github_events;"
  ```
* **Technical Note:** The table is an `EXTERNAL` table. Deleting storage files leaves the metastore out of sync. Because the Presto Hudi connector is read-only, `DROP TABLE` DDL must be submitted through Spark SQL.

#### Presto query fails with an S3 Authentication / Access Denied error
* **Cause:** Presto cannot communicate with MinIO storage, usually because `.env` was missing or modified after the container started.
* **Resolution:** Check that Presto received the active credentials:
  ```bash
  docker exec presto-server cat /tmp/etc/catalog/hudi.properties
  ```
  You should see `hive.s3.endpoint`, `hive.s3.aws-access-key`, and `hive.s3.aws-secret-key`. If missing, recreate the Presto cluster:
  ```bash
  docker compose restart presto-server presto-worker-1 presto-worker-2
  ```
* **Technical Note:** Presto's entrypoint dynamically copies read-only mounted files to `/tmp/etc` and appends `hive.s3.*` credentials from environment variables before launching.

---

### 4. Platform Resets

| Reset Type | Command / Action | What gets deleted | What is preserved |
| :--- | :--- | :--- | :--- |
| **Data-Only Reset** *(Recommended)* | 1. In [MinIO Console](http://localhost:9001), delete `hudi-tables/`<br>2. Run `DROP TABLE IF EXISTS default.github_events;` in Spark | Ingested Hudi tables and raw data batches | Docker images, Ivy JAR cache, Airflow DAG history |
| **Factory Reset** *(Clean Slate)* | `docker compose down -v`<br>`docker compose up -d` | **Everything** — all 6 volumes, tables, users, and logs | Nothing (fresh initial state) |
