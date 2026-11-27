# Build Turbofan Telemetry from zero: a step-by-step guide

Every file below is the project's real code. Chapters:

- [0. Why this project exists and why it is good](#why)
- [1. The big picture and the words you need](#words)
- [2. Set up your computer](#setup)
- [3. Run Kafka with Docker](#kafka)
- [4. Get and understand the NASA data](#data)
- [5. Configuration, the event contract and the health logic](#contract)
- [6. Unit tests first](#tests)
- [7. Calibrate the alerts on real failures](#calibrate)
- [8. Create the Kafka topic and the command line](#topic)
- [9. The producer: replay the fleet, with chaos](#producer)
- [10. The Spark session](#spark)
- [11. Bronze: store exactly what arrived](#bronze)
- [12. Silver: validate, dead-letter, deduplicate](#silver)
- [13. Gold: engine state, alerts and fleet health](#gold)
- [14. Run the streaming application](#run)
- [15. Report and verify: prove it](#verify)
- [16. The crash demo: exactly-once under chaos](#demo)
- [17. Dashboard, Docker image and CI](#dashboard)
- [18. Load test and capacity plan](#loadtest)
- [19. Troubleshooting: every error you might hit](#trouble)
- [20. Document it and publish to GitHub](#docs)
- [21. Explain it in an interview](#talk)


<a id="why"></a>

# 0. Why this project exists, and why it is a strong project

Read this before you install anything. If you can explain this page in your own words, you can explain the whole project in an interview.

## The problem in one paragraph

Cummins sells engines that run in trucks, mines, ships and generators. Modern engines are full of sensors (temperatures, pressures, speeds) and send readings over the network all the time. When an engine wears out and fails in the field, it is expensive: the truck stops, the customer loses money, a technician has to travel, and the failure may become a **warranty claim**. If you can see an engine **getting worse before it fails**, you can plan maintenance instead of reacting to a breakdown. That is called **predictive maintenance**.

To do it, a company needs a **data pipeline**: a system that takes the sensor readings as they arrive, cleans them, keeps them safely, computes an **engine health score**, and raises an **alert** early enough to act. That pipeline must be reliable: no readings lost, no readings counted twice, bad data must not break it, and it must recover if a machine crashes.

**This project builds exactly that pipeline, on real public engine data, on your laptop, for free.**

## What you will have at the end

- A **live stream** of 200 jet engines (NASA's public C-MAPSS dataset, every engine run until it failed) replayed as if the engines were flying right now.
- **Kafka** receiving the readings, **Spark Structured Streaming** processing them, **Delta Lake** tables storing them in three layers (**bronze → silver → gold**).
- A **health score** per engine and **alerts** (WARNING, then CRITICAL) that fire a median of **44 cycles (flights) before failure** for **every one** of the 200 engines.
- **Proof** that nothing is lost or duplicated even when you inject duplicates, late messages and broken messages **and kill the processing program in the middle** (`kill -9`): 54,787 expected events, 54,787 stored, 0 missing, 0 duplicates.
- A **dead-letter table** where every bad message is kept with a reason, instead of crashing the pipeline.
- A **load test** that measures how fast it is and finds the bottleneck.
- **Unit tests**, a **CI pipeline** on GitHub, a **dashboard**, and **documentation** (architecture, decisions, a failure log).

## Why this is a good project (what a reviewer sees)

Most student data projects are "read a CSV, transform it, write a CSV". They show you can use a library. They do not show you can run something in production. This project is designed to show production thinking:

| What most projects show | What this project shows | Why an interviewer cares |
|---|---|---|
| A pipeline that runs once | A pipeline that **keeps running**, survives a crash and resumes exactly where it stopped | Real pipelines fail every week. Recovery is the job. |
| "It works" | "It works, **and here is the proof**" (a manifest of every event compared with the output) | Claims are cheap. Verification is rare and convincing. |
| Clean input | Input with **duplicates, late data and garbage** injected on purpose | Real sources are dirty. Handling it is most of data engineering. |
| One script | **Layers** (bronze/silver/gold) with clear jobs | This is how Databricks lakehouses at Cummins-sized companies are organised. |
| No numbers | **Measured** throughput, latency percentiles, a found bottleneck, a capacity plan | Shows you can reason about scale with evidence. |
| No mistakes shown | **16 documented failures** with cause and fix | Shows debugging skill and honesty. |
| Random fake data | **Real NASA data**, public and verifiable | Anyone can check your results. |
| A model that "looks right" | A model **evaluated per fleet** that revealed a 44% miss on one fault type, then fixed it (56% → 100%) | Shows you check results by segment, not just on average. |

## Why it fits the Cummins Data Engineer 1 posting

| Posting line | Where this project covers it |
|---|---|
| Ingest **event-based** data | Kafka producer and consumer, partitions, keys |
| Build reliable, scalable pipelines **with monitoring and alerts** | Streaming queries, checkpoints, progress metrics, alerts table, report command |
| Monitor and troubleshoot **data quality and integrity** | Validation rules, dead-letter table with reason codes, exactly-once verification |
| **Spark, Kafka**, big data open source | Spark Structured Streaming, Kafka, Delta Lake |
| **Physical data models** and storage architecture | Bronze/silver/gold tables, partitioning, table grains |
| **Testing and troubleshooting** | 14 unit tests, a parity test, a crash test, a load test, the failure log |
| **IoT technology** | Engine telemetry, event time, late data, per-device ordering |
| **AI-ready data** | Clean per-cycle features and labels suitable for failure prediction models |
| **DevOps / CI** | Docker, GitHub Actions CI |

## Why each big choice was made (short version, details in each chapter)

- **Streaming instead of batch:** engines send data continuously and alerts are only useful if they are early. A nightly batch job would warn a day late.
- **Kafka:** the standard way companies move events between systems. It stores messages for days, so you can replay them, and it keeps each engine's readings in order. (Azure Event Hubs, which Cummins is likely to use, speaks the same Kafka protocol.)
- **Spark Structured Streaming:** the same engine data teams already use for batch on Databricks, so one skill covers both. It has checkpoints for crash recovery.
- **Delta Lake:** turns plain files into reliable tables (no half-written files, updates with MERGE, history). It is the default table format on Databricks.
- **Bronze/silver/gold:** keep the raw data untouched (so you can always rebuild), clean it once, and serve business-ready tables.
- **NASA C-MAPSS data:** real engines run until failure, public, no login, and the right domain (engines) for Cummins. Never use employer data in a personal project.
- **Everything local and free:** anyone can run it with Docker and Python, no cloud account, no cost. The same code runs on Databricks by changing paths.

## How to use this guide

1. Do the chapters **in order**. Each one ends with a **Check** step: do not continue until the check passes.
2. Every file is given **complete**. Create the file with exactly that name and content. Read the explanation under it before moving on; that is what you will be asked about.
3. Every technology has a box: **What it is, Why here, Alternatives, Advantages, Impact**. Learn those boxes; they are your interview answers.
4. When something breaks, look at the **Troubleshooting** chapter first: the same errors happened when this project was first built.
5. Type the code where you can instead of only copying it. Typing makes you read every line.

**Time needed:** about 2-3 days for a first build if you are new, plus a day to run the tests and write your own notes.

**An honest note on ownership:** after you build this yourself following the guide, you can truthfully say you built and verified it. In an interview, be ready to explain any line and any decision; if you followed a guide for parts of it, say so plainly and focus on what you understood, changed and measured yourself. Interviewers respect that far more than a claim that falls apart under questions.

---

<a id="words"></a>

# 1. The big picture, and the words you need

## The flow, in plain words

```
NASA data file ──► Producer (Python) ──► Kafka topic ──► Spark: Bronze ──► Silver ──► Gold ──► Dashboard
 200 engines       sends one reading     a durable        raw copy       clean,       health,     charts
 run to failure    per engine per tick   queue/log        of everything  checked,     alerts,
                   (+ chaos on purpose)                                   no dupes     fleet stats
                                                           Silver ──► Dead-letter table (bad messages + reason)
```

1. A **producer** program reads the NASA file and sends one JSON message per engine reading into **Kafka**, as if 200 engines were flying right now. On purpose, it also sends some duplicates, late messages and broken messages.
2. **Spark** reads Kafka continuously in small batches every 5 seconds (**micro-batches**).
3. **Bronze** stores every message exactly as received.
4. **Silver** checks each message. Good ones are parsed into columns, de-duplicated and given a health score. Bad ones go to a **dead-letter** table with a reason.
5. **Gold** keeps one row per engine (latest health, alert level), an alerts table (each alert once), and per-minute fleet statistics.
6. A **report/verify** command proves nothing was lost or duplicated. A **dashboard** shows the fleet.

## Words you must know (one line each)

| Word | Meaning |
|---|---|
| **Event / message** | One reading from one engine for one cycle, as JSON |
| **Cycle** | One flight of the engine. Engines live 128-362 cycles in this data |
| **Producer / consumer** | A program that writes to / reads from Kafka |
| **Topic** | A named stream in Kafka (ours: `engine-telemetry`) |
| **Partition** | A topic is split into partitions so several readers work in parallel. Order is kept **inside** a partition |
| **Key** | A value attached to each message (ours: `engine_id`). Same key → same partition → that engine's readings stay in order |
| **Offset** | The position number of a message inside a partition |
| **Micro-batch** | Spark processes whatever arrived in the last few seconds as one small batch |
| **Checkpoint** | A folder where Spark records what it has processed, so after a crash it continues from there |
| **Delta table** | A folder of Parquet files plus a transaction log that makes it behave like a reliable database table |
| **Bronze / silver / gold** | Raw → clean → business-ready layers (the "medallion" pattern) |
| **Dead-letter table** | Where bad records go, with the reason, instead of being dropped or crashing the job |
| **Event time** | When the reading happened (set by the producer), as opposed to when we received it |
| **Watermark** | How long Spark waits for late events before treating a time period as complete |
| **Idempotent** | Doing it twice gives the same result as once. The key to safe retries |
| **Exactly-once** | Every event affects the result exactly one time, even with retries and crashes |
| **EWMA** | A smoothed average that gives more weight to recent values; used to smooth the noisy health score |
| **MERGE (upsert)** | Update the row if the key exists, insert if not |

## The final folder layout (you will create all of it)

```
turbofan-telemetry/
  requirements.txt  docker-compose.yml  Dockerfile  .gitignore  README.md
  telemetry/
    __init__.py  config.py  cmapss.py  events.py  admin.py  producer.py  spark.py  report.py  cli.py
    jobs/  __init__.py  bronze.py  silver.py  gold.py  run.py
  tests/  test_events.py  test_spark_parity.py
  scripts/  calibrate.py  demo.sh  loadtest.py
  dashboard/  app.py
  docs/  architecture.md  decisions.md  build-log.md  capacity-plan.md
  .github/workflows/ci.yml
  data/   lake/   (created by the programs, never committed to Git)
```

**Check:** you can explain, without looking, what bronze, silver and gold each do, and why the message key is the engine id.

---

<a id="setup"></a>

# 2. Set up your computer

You need five tools. Install them once.

| Tool | Version | Why |
|---|---|---|
| Git | any recent | Version control, push to GitHub |
| Python | **3.12** (3.10-3.13 work) | The language of the whole project |
| Java (JDK) | **17 or 21** | Spark runs on the Java Virtual Machine, even when you write Python |
| Docker Desktop | any recent | Runs Kafka in a container, no Kafka install needed |
| VS Code | any | Editor (any editor works) |

**Windows users:** install **WSL2 with Ubuntu** (`wsl --install` in an admin PowerShell, then restart) and run every command in this guide inside the Ubuntu terminal. Spark and shell scripts are much easier there. Install Docker Desktop on Windows and enable "Use the WSL 2 based engine".

**macOS:** install Homebrew, then `brew install git python@3.12 openjdk@17` and Docker Desktop.

**Ubuntu / WSL:**

```bash
sudo apt update && sudo apt install -y git python3.12 python3.12-venv openjdk-17-jdk
```

### Tech box: Docker
- **What it is:** a tool that runs software in isolated "containers" built from images, so it behaves the same on every machine.
- **Why here:** Kafka is a Java server with many settings. With Docker you start a correct, pinned Kafka with one command and delete it just as easily.
- **Alternatives:** installing Kafka by hand (slow, error-prone), a cloud Kafka (needs an account and money).
- **Advantages:** reproducible, no system clutter, the same file works for every reviewer.
- **Impact:** without it, "works on my machine" problems would eat days.

### Tech box: Java for a Python project
- **What it is:** Spark is written in Scala and runs on the JVM. PySpark is a Python front end that talks to that JVM.
- **Why it matters:** if Java is missing or the wrong version, Spark will not start (`JAVA_HOME is not set`, `UnsupportedClassVersionError`).
- **Rule:** Spark 4 needs Java 17 or 21.

## Check that everything works

```bash
git --version
python3 --version        # 3.12.x (3.10-3.13 fine)
java -version            # 17 or 21
docker info              # must print "Server Version". If it errors, open Docker Desktop (daemon not running)
```

## Create the project folder and a virtual environment

```bash
mkdir turbofan-telemetry && cd turbofan-telemetry
git init
python3 -m venv .venv
source .venv/bin/activate          # Windows WSL is the same. Your prompt now starts with (.venv)
mkdir -p telemetry/jobs tests scripts dashboard docs .github/workflows
touch telemetry/__init__.py telemetry/jobs/__init__.py
```

### Tech box: virtual environment (venv)
- **What:** a private folder of Python packages for this project only.
- **Why:** this project pins exact versions (Spark 4.0.1 etc.). Installing them globally can break other projects, and other projects can break this one.
- **Alternatives:** conda, poetry, uv. venv is built into Python and enough here.
- **Impact:** without it, the "Python workers use the wrong interpreter" bug (F9 in Troubleshooting) is much more likely.

## requirements.txt

Create `requirements.txt`:

```text
pyspark==4.0.1
delta-spark==4.0.0
confluent-kafka==2.8.0
pandas==2.2.3
pyarrow==19.0.1
deltalake==0.25.5
streamlit==1.41.1
pytest==8.3.4
```

**Every line explained:**

| Package | What it does here | Why this one |
|---|---|---|
| `pyspark==4.0.1` | The stream processor | Spark 4 supports Python 3.13 and Java 17/21. Spark 3.5 does not support Python 3.13 (F4) |
| `delta-spark==4.0.0` | Delta Lake for Spark | Delta versions are built for one Spark version: 4.0.0 matches Spark 4.0 |
| `confluent-kafka==2.8.0` | Python Kafka client (producer, admin) | Built on librdkafka (C): much faster than `kafka-python` and supports the idempotent producer |
| `pandas==2.2.3`, `pyarrow==19.0.1` | Used by Spark's `applyInPandas` in gold, and by the report | pandas UDFs move data with Arrow |
| `deltalake==0.25.5` | delta-rs: reads Delta tables **without Spark** | The report, verify and dashboard start in milliseconds instead of waiting for a JVM |
| `streamlit==1.41.1` | Dashboard | A web dashboard in one Python file |
| `pytest==8.3.4` | Tests | The standard Python test runner |

**Why pin exact versions (`==`)?** So the project behaves the same next year. Spark, Delta and the Kafka connector must match each other exactly; an unpinned upgrade is the most common reason a data project stops working.

```bash
pip install -r requirements.txt
```

## .gitignore

Create `.gitignore`:

```text
data/
lake/
.venv/
__pycache__/
*.pyc
spark-warehouse/
metastore_db/
derby.log
.pytest_cache/
```

`data/` and `lake/` are created by the programs (downloaded data, Delta tables, checkpoints). They are large, machine-specific and reproducible, so they never go into Git.

**Check:** `python -c "import pyspark, delta, confluent_kafka; print(pyspark.__version__)"` prints `4.0.1`.

---

<a id="kafka"></a>

# 3. Run Kafka with Docker

### Tech box: Apache Kafka
- **What it is:** a distributed, durable **log** of events. Producers append messages to a topic; consumers read them at their own pace; messages stay for a retention period (ours: 7 days) whether or not they were read.
- **Why here:** engines produce a continuous stream. Kafka absorbs bursts, keeps each engine's readings in order (by key), lets several consumers read the same data, and lets you **replay** after a bug or crash.
- **Alternatives:** **Azure Event Hubs** / AWS Kinesis (managed cloud versions; Event Hubs even speaks the Kafka protocol, so this code works against it), RabbitMQ (a queue: messages are deleted once consumed, no replay), writing files to a folder (no ordering, no backpressure, slow).
- **Advantages:** replay, ordering per key, parallelism through partitions, huge throughput, industry standard.
- **Impact:** without a durable log, a crash in the processor would lose whatever was in flight, and you could not reprocess history.

### Tech box: KRaft mode
Old Kafka needed a second system, **ZooKeeper**, to manage the cluster. Since Kafka 3.3 it can manage itself with its own consensus protocol (**KRaft**), and **Kafka 4.0 removed ZooKeeper completely**. So we run one container that is both broker and controller. Simpler, and current.

Create `docker-compose.yml`:

```yaml
# Local stack: Kafka (KRaft mode, no ZooKeeper) + Kafka UI. No accounts or logins needed.
#   docker compose up -d          -> Kafka on localhost:9092, Kafka UI on http://localhost:8080
# The optional `app` and `dashboard` services run the Python/Spark code in a container,
# so you don't need Java or Python installed locally (see README, "Option B").
services:
  kafka:
    image: apache/kafka:3.9.0
    container_name: tt-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:19092,PLAINTEXT_HOST://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:19092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_LOG_RETENTION_HOURS: 168
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1"]
      interval: 10s
      timeout: 10s
      retries: 10

  kafka-ui:
    image: provectuslabs/kafka-ui:v0.7.2
    container_name: tt-kafka-ui
    depends_on: [kafka]
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:19092

  # ---- Option B: run the code inside Docker (no local Java/Python needed) ----
  app:
    build: .
    profiles: ["app"]
    depends_on: [kafka]
    working_dir: /app
    volumes:
      - .:/app
      - ivy-cache:/root/.ivy2          # Spark's downloaded Kafka/Delta jars survive restarts
    environment:
      KAFKA_BOOTSTRAP: kafka:19092
    ports:
      - "4040:4040"                    # Spark UI
    entrypoint: ["python", "-m", "telemetry.cli"]

  dashboard:
    build: .
    profiles: ["app"]
    working_dir: /app
    volumes:
      - .:/app
    ports:
      - "8501:8501"
    command: ["streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0", "--server.headless=true"]

volumes:
  ivy-cache:
```

**What each important setting means:**

| Setting | Meaning |
|---|---|
| `image: apache/kafka:3.9.0` | Official Apache image, **pinned** version. Never use `latest` in a project others must reproduce |
| `KAFKA_PROCESS_ROLES: broker,controller` | One node does both jobs (KRaft, no ZooKeeper) |
| `KAFKA_LISTENERS` / `ADVERTISED_LISTENERS` | Kafka gives clients an address to reconnect to. `localhost:9092` for programs on your laptop, `kafka:19092` for programs inside Docker. Getting this wrong is the #1 "cannot connect to Kafka" problem |
| `*_REPLICATION_FACTOR: 1` | We have one broker, so internal topics can only have one copy. In production: 3 |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` | Topics must be created on purpose (with our partition count). Prevents typos silently creating new topics |
| `KAFKA_LOG_RETENTION_HOURS: 168` | Keep messages 7 days, so you can replay |
| `healthcheck` | Docker knows when Kafka is really ready |
| `kafka-ui` | A web page to look inside Kafka (topics, partitions, messages) |
| `app`, `dashboard` with `profiles: ["app"]` | Optional: run the Python code inside Docker if you cannot install Java/Python (Option B, chapter 17). Not started by default |

```bash
docker compose up -d
docker compose ps          # tt-kafka should be "healthy" after ~20-30 s
```

Open **http://localhost:8080**: Kafka UI. There are no topics yet.

**Check:** `docker compose ps` shows `tt-kafka` healthy and Kafka UI loads in the browser.

---

<a id="data"></a>

# 4. Get and understand the NASA data

### Tech box: NASA C-MAPSS
- **What it is:** NASA simulated turbofan (jet) engines with a physics model called C-MAPSS. Every engine starts healthy and runs until it fails. One row = one cycle (≈ one flight): engine number, cycle number, 3 operating settings and 21 sensor readings.
- **Why here:** real engine degradation, public and free (no login), small (12 MB), widely used in predictive-maintenance research, and in Cummins' domain (engines).
- **Alternatives:** random fake data (no real signal to find, so the alerts would mean nothing), employer data (**never**: confidential), Kaggle copies (need a login).
- **Impact:** because the data is real, your "44 cycles of warning" result is a real, checkable finding.

**The four subsets:**

| Subset | Engines | Operating conditions | Fault types |
|---|---|---|---|
| **FD001** | 100 | 1 | 1 (high-pressure compressor, "HPC") |
| FD002 | 260 | 6 | 1 |
| **FD003** | 100 | 1 | 2 (HPC **and** fan) |
| FD004 | 248 | 6 | 2 |

**We use FD001 + FD003.** They run at one operating condition, so when a sensor drifts, it is because of wear. In FD002/FD004 the same sensor value means different things at different altitudes and speeds, so you would first need per-condition normalisation (a good extension later).

## The data module

Create `telemetry/cmapss.py`:

```python
"""Download and parse NASA's C-MAPSS turbofan engine degradation dataset.

Source: NASA Prognostics Center of Excellence data repository (public, no login).
Each row = one operating cycle (flight) of one engine, run until failure.
Columns: unit, cycle, 3 operational settings, 21 sensor readings.
"""
import io
import random
import urllib.request
import zipfile

from .config import DATA_DIR

URL = "https://phm-datasets.s3.amazonaws.com/NASA/6.+Turbofan+Engine+Degradation+Simulation+Data+Set.zip"
SENSORS = [f"s{i}" for i in range(1, 22)]
COLUMNS = ["unit", "cycle", "op_setting_1", "op_setting_2", "op_setting_3"] + SENSORS
# FD001 and FD003 run at a single operating condition, so sensor drift = wear.
# (FD002/FD004 mix six conditions and would need per-condition normalisation.)
DEFAULT_SUBSETS = ("FD001", "FD003")


def download(force: bool = False) -> None:
    """Fetch the zip once and extract train_FD00x.txt files into data/cmapss/."""
    out = DATA_DIR / "cmapss"
    if (out / "train_FD001.txt").exists() and not force:
        print(f"already present: {out}")
        return
    out.mkdir(parents=True, exist_ok=True)
    print(f"downloading {URL} ...")
    with urllib.request.urlopen(URL, timeout=120) as r:
        outer = zipfile.ZipFile(io.BytesIO(r.read()))
    inner_name = next(n for n in outer.namelist() if n.endswith("CMAPSSData.zip"))
    inner = zipfile.ZipFile(io.BytesIO(outer.read(inner_name)))
    for name in inner.namelist():
        if name.endswith(".txt"):
            (out / name).write_bytes(inner.read(name))
    print(f"extracted to {out}")


def load(subset: str) -> list[dict]:
    """Rows of train_<subset>.txt as dicts. Falls back to synthetic data if the file is missing."""
    path = DATA_DIR / "cmapss" / f"train_{subset}.txt"
    if not path.exists():
        print(f"{path} not found: using synthetic engines (run `python -m telemetry.cli download` for real NASA data)")
        return synthetic(subset)
    rows = []
    for line in path.read_text().split("\n"):
        parts = line.split()
        if not parts:
            continue
        vals = [float(x) for x in parts[: len(COLUMNS)]]
        row = dict(zip(COLUMNS, vals))
        row["unit"], row["cycle"] = int(row["unit"]), int(row["cycle"])
        rows.append(row)
    return rows


# Baseline means used by the synthetic generator (from healthy FD001 cycles)
_BASE = {"s1": 518.67, "s2": 642.38, "s3": 1586.9, "s4": 1402.8, "s5": 14.62, "s6": 21.61, "s7": 553.95, "s8": 2388.06,
         "s9": 9055.8, "s10": 1.3, "s11": 47.36, "s12": 521.9, "s13": 2388.05, "s14": 8137.4, "s15": 8.418, "s16": 0.03,
         "s17": 392.3, "s18": 2388.0, "s19": 100.0, "s20": 38.93, "s21": 23.36}
_DRIFT = {"s2": 1.3, "s3": 16, "s4": 28, "s7": -2.6, "s9": 43, "s11": 0.84, "s12": -2.2, "s14": 31, "s15": 0.105,
          "s17": 4.5, "s20": -0.53, "s21": -0.31}


def synthetic(subset: str, engines: int = 100, seed: int = 7) -> list[dict]:
    """Engines whose degrading sensors drift exponentially toward failure, same shape as C-MAPSS."""
    rnd = random.Random(f"{subset}-{seed}")
    rows = []
    for unit in range(1, engines + 1):
        life = rnd.randint(128, 360)
        for cycle in range(1, life + 1):
            wear = (cycle / life) ** 3
            row = {"unit": unit, "cycle": cycle, "op_setting_1": rnd.gauss(0, 0.002), "op_setting_2": rnd.gauss(0, 0.0003), "op_setting_3": 100.0}
            for s in SENSORS:
                noise = abs(_BASE[s]) * 0.0008 if s in _DRIFT else 0
                row[s] = round(_BASE[s] + _DRIFT.get(s, 0) * wear + rnd.gauss(0, noise), 4)
            rows.append(row)
    return rows
```

**Read it like this:**
- `URL`: the official NASA Prognostics Center mirror. The first link tried (on data.nasa.gov) was dead (F3). External links move: pin the source and document it.
- `download()`: downloads the zip once, extracts the inner `CMAPSSData.zip`, and writes the `.txt` files to `data/cmapss/`. It skips the download if the files already exist (safe to run twice: **idempotent**).
- `load(subset)`: turns each line of `train_FD001.txt` into a dictionary like `{"unit": 7, "cycle": 113, "s1": 518.67, ...}`.
- `synthetic()`: if the file is missing (no internet, link moved), it generates fake engines with the **same shape** and realistic drift so the project still runs. A **fallback** is good engineering for any external dependency.

It needs `config.py` for the `DATA_DIR` path. Create `telemetry/config.py` now (explained fully in chapter 5):

```python
"""Central configuration. Override anything with environment variables."""
import os
from pathlib import Path

ROOT = Path(os.environ.get("TT_ROOT", Path(__file__).resolve().parents[1]))
DATA_DIR = ROOT / "data"
LAKE = Path(os.environ.get("TT_LAKE", ROOT / "lake"))

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("TT_TOPIC", "engine-telemetry")
PARTITIONS = int(os.environ.get("TT_PARTITIONS", "6"))

# Delta tables (bronze / silver / gold) and their streaming checkpoints
TABLES = {
    "bronze": LAKE / "bronze" / "telemetry_raw",
    "silver": LAKE / "silver" / "telemetry",
    "dlq": LAKE / "silver" / "dead_letter",
    "engine_state": LAKE / "gold" / "engine_current_state",
    "alerts": LAKE / "gold" / "engine_alerts",
    "fleet_minute": LAKE / "gold" / "fleet_health_minute",
}
CHECKPOINTS = LAKE / "_checkpoints"

# Streaming behaviour
WATERMARK = os.environ.get("TT_WATERMARK", "10 minutes")      # how late an event may arrive
TRIGGER = os.environ.get("TT_TRIGGER", "5 seconds")            # micro-batch interval
MAX_OFFSETS_PER_TRIGGER = int(os.environ.get("TT_MAX_OFFSETS", "200000"))  # backpressure cap
```

Download the data:

```bash
python -c "from telemetry import cmapss; cmapss.download()"
ls data/cmapss            # train_FD001.txt ... train_FD004.txt, test_*, RUL_*
```

## Look at the data yourself (do this, it is where the design comes from)

```bash
python - <<'EOF'
from statistics import mean
from telemetry.cmapss import load, SENSORS
rows = load("FD001")
by = {}
for r in rows: by.setdefault(r["unit"], []).append(r)
first = [rs[0] for rs in by.values()]; last = [max(rs, key=lambda r: r["cycle"]) for rs in by.values()]
print("engines:", len(by), "| life (cycles) min/avg/max:", min(len(v) for v in by.values()), round(mean(len(v) for v in by.values())), max(len(v) for v in by.values()))
for s in SENSORS:
    a, b = mean(r[s] for r in first), mean(r[s] for r in last)
    print(f"{s:>4}  first cycle {a:10.3f}   last cycle {b:10.3f}   {'RISES' if b > a * 1.0005 else 'FALLS' if b < a * 0.9995 else 'flat'}")
EOF
```

**What you will see** (this is what the health score is built on):
- Engines live **128-362 cycles**, about 206 on average.
- **s1, s5, s6, s10, s16, s18, s19** are flat: no information, ignored.
- **s2, s3, s4, s11, s15, s17 rise** as the engine wears (temperatures and pressures go up).
- **s7, s12, s20, s21 fall**.
- **s8, s9, s13, s14** are the fan and core **speed** sensors. s9 and s14 also rise, s8 and s13 barely move. The project leaves the four speed sensors out and builds the score from the **10 temperature, pressure and flow sensors** above, which drift cleanly with wear. (Trying the speed sensors in the score is a good experiment once everything works: measure it with the calibration script in chapter 7.)

That table is the whole idea of the health score in chapter 5: measure how far the 10 drifting sensors have moved from their healthy values.

**Check:** the script prints 100 engines and you can name which sensors rise and which fall.

---

<a id="contract"></a>

# 5. Configuration, the event contract and the health logic

This chapter is pure Python: no Kafka, no Spark. It holds the **rules** of the system (what an event looks like, what makes it valid, how health is computed, when to alert). Keeping rules separate from the big tools is the single best design decision in the project: the rules can be tested in milliseconds and reused by Spark, the tests and the calibration script.

## config.py (you created it in chapter 4)

Every path, name and tuning number lives here, and each can be overridden with an environment variable (for example `TT_TRIGGER="1 second"`). Nothing is hard-coded in the jobs.

| Setting | Value | Why |
|---|---|---|
| `TOPIC` / `PARTITIONS` | `engine-telemetry`, 6 | 6 partitions → up to 6 parallel readers; enough for a laptop, small enough to see in Kafka UI |
| `TABLES` | bronze, silver, dlq, three gold tables | Each table is a folder under `lake/` |
| `CHECKPOINTS` | `lake/_checkpoints` | One sub-folder per streaming query. **Never share or delete a checkpoint casually**: it is the query's memory |
| `WATERMARK` | 10 minutes | How late an event may arrive and still be de-duplicated |
| `TRIGGER` | 5 seconds | Micro-batch interval: a balance between latency and overhead |
| `MAX_OFFSETS_PER_TRIGGER` | 200,000 | **Backpressure**: after an outage, Spark catches up in batches of at most 200k instead of swallowing hours at once |

**Why environment variables?** The same code then runs on your laptop, in Docker and on Databricks with different settings and no code change (the "twelve-factor" principle).

## events.py: the contract and the rules

Create `telemetry/events.py`:

```python
"""Event contract and pure domain logic (no Spark, no Kafka): unit-testable in milliseconds."""
import json
import math
import uuid
from datetime import datetime, timezone

from .cmapss import SENSORS

SCHEMA_VERSION = 1
NAMESPACE = uuid.UUID("6f1d6a2e-2b6a-4f7e-9a51-3c1b2b9d7e10")

# Sensors that drift with wear, and their direction (+1 rises, -1 falls as the engine degrades).
# Baselines = mean / std of the first 30 (healthy) cycles of every FD001 engine.
HEALTH_SENSORS = {
    "s2": (+1, 642.3832, 0.3795), "s3": (+1, 1586.8985, 4.6482), "s4": (+1, 1402.831, 5.9308),
    "s7": (-1, 553.9477, 0.6237), "s11": (+1, 47.3561, 0.1769), "s12": (-1, 521.9049, 0.5068),
    "s15": (+1, 8.4184, 0.0264), "s17": (+1, 392.27, 1.1424), "s20": (-1, 38.9323, 0.1296),
    "s21": (-1, 23.3592, 0.0787),
}

# C-MAPSS FD003 has a SECOND fault mode (fan degradation). Its signature differs from the HPC fault above:
# s7, s12, s20, s21 rise instead of falling and s15 falls instead of rising (build-log F14, Milestone T1).
FAN_SIGNS = {"s2": +1, "s3": +1, "s4": +1, "s7": +1, "s11": +1, "s12": +1,
             "s15": -1, "s17": +1, "s20": +1, "s21": +1}
# The fan pattern moves s7/s12 by 20+ standard deviations, so it is down-weighted to give the same lead times and
# false-alarm rate as the HPC pattern. Tuned on odd FD003 engines, checked on even ones (scripts/calibrate.py).
FAN_WEIGHT = 0.4

# Physically plausible ranges. Anything outside is a sensor/transport fault -> dead-letter.
SENSOR_RANGES = {s: (0.0, 20000.0) for s in SENSORS}
SENSOR_RANGES.update({"s11": (40.0, 60.0), "s4": (1200.0, 1600.0), "s2": (600.0, 700.0)})

# Alert policy, calibrated on FD001 run-to-failure data (see docs/decisions.md, ADR-004):
EWMA_ALPHA = 0.2
WARNING_AT = 1.5     # median lead ~44 cycles before failure
CRITICAL_AT = 2.5    # median lead ~19 cycles before failure
LEVELS = {0: "OK", 1: "WARNING", 2: "CRITICAL"}


def engine_id(subset: str, unit: int, loop: int = 0) -> str:
    return f"{subset}-{unit:03d}" + (f"-L{loop}" if loop else "")


def event_id(engine: str, cycle: int) -> str:
    """Deterministic: the same engine+cycle always gets the same id, so duplicates are detectable."""
    return str(uuid.uuid5(NAMESPACE, f"{engine}:{cycle}"))


def make_event(subset: str, row: dict, loop: int = 0, now: datetime | None = None) -> dict:
    eng = engine_id(subset, row["unit"], loop)
    now = now or datetime.now(timezone.utc)
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id(eng, row["cycle"]),
        "fleet": subset,
        "engine_id": eng,
        "cycle": int(row["cycle"]),
        "event_time": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "op_settings": [row["op_setting_1"], row["op_setting_2"], row["op_setting_3"]],
        "sensors": {s: row[s] for s in SENSORS},
    }


def to_bytes(event: dict) -> bytes:
    return json.dumps(event, separators=(",", ":")).encode()


def validate(event: dict | None) -> str | None:
    """Return None if valid, else a dead-letter reason code. Mirrors the Spark silver rules."""
    if not isinstance(event, dict):
        return "MALFORMED_JSON"
    for f in ("event_id", "engine_id", "cycle", "event_time", "sensors"):
        if event.get(f) in (None, ""):
            return "MISSING_FIELD"
    sensors = event["sensors"]
    for s in SENSORS:
        v = sensors.get(s)
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return "MISSING_FIELD"
        lo, hi = SENSOR_RANGES[s]
        if not lo <= v <= hi:
            return "OUT_OF_RANGE"
    return None


def _index(sensors: dict, signs: dict) -> float:
    total = 0.0
    for s, (_, mu, sd) in HEALTH_SENSORS.items():
        total += signs[s] * (sensors[s] - mu) / sd
    return total / len(HEALTH_SENSORS)


HPC_SIGNS = {s: sign for s, (sign, _, _) in HEALTH_SENSORS.items()}


def hpc_index(sensors: dict) -> float:
    """Signed z-score average for the HPC (compressor) fault pattern."""
    return _index(sensors, HPC_SIGNS)


def fan_index(sensors: dict) -> float:
    """Signed z-score average for the fan fault pattern."""
    return _index(sensors, FAN_SIGNS)


def health_index(sensors: dict) -> float:
    """Fault-mode aware health index (v2): the worse of the HPC and fan patterns.

    ~0 when healthy, rises with wear of either kind. v1 used only the HPC pattern, so fan-fault
    engines cancelled themselves out and 44% of FD003 never alerted (build-log F14).
    """
    return max(hpc_index(sensors), FAN_WEIGHT * fan_index(sensors))


def fault_mode(sensors: dict) -> str:
    """Which pattern dominates: useful for routing the alert to the right maintenance team."""
    return "FAN" if FAN_WEIGHT * fan_index(sensors) > hpc_index(sensors) else "HPC"


def level_for(ewma: float) -> int:
    return 2 if ewma >= CRITICAL_AT else 1 if ewma >= WARNING_AT else 0


def advance_state(prev: dict | None, events: list[dict]) -> tuple[dict | None, list[dict]]:
    """Fold a batch of one engine's events into its state. Returns (new_state, alerts_raised).

    - Events are applied in cycle order.
    - Events at or before the last applied cycle are ignored: this makes re-processing a
      micro-batch (after a crash) idempotent, and drops stale out-of-order cycles.
    - An alert is raised only when the level escalates (OK->WARNING, ->CRITICAL).
    """
    state = dict(prev) if prev else None
    alerts = []
    for e in sorted(events, key=lambda x: x["cycle"]):
        if state and e["cycle"] <= state["last_cycle"]:
            continue
        hi = e["health_index"]
        ewma = hi if not state else EWMA_ALPHA * hi + (1 - EWMA_ALPHA) * state["ewma_health"]
        prev_level = state["alert_level"] if state else 0
        lvl = max(prev_level, level_for(ewma))  # levels only escalate until maintenance resets the engine
        state = {"engine_id": e["engine_id"], "fleet": e["fleet"], "last_cycle": e["cycle"],
                 "last_event_time": e["event_time"], "health_index": hi, "ewma_health": ewma,
                 "alert_level": lvl, "s11": e["s11"], "s4": e["s4"], "fault_mode": e.get("fault_mode")}
        if lvl > prev_level:
            alerts.append({"alert_id": f"{e['engine_id']}:{LEVELS[lvl]}", "engine_id": e["engine_id"], "fleet": e["fleet"],
                           "level": LEVELS[lvl], "cycle": e["cycle"], "ewma_health": ewma, "event_time": e["event_time"],
                           "fault_mode": e.get("fault_mode")})
    return state, alerts
```

### Part 1: the event (`make_event`)

One message looks like this:

```json
{"schema_version": 1, "event_id": "6c1f...", "fleet": "FD001", "engine_id": "FD001-007",
 "cycle": 113, "event_time": "2026-11-25T14:56:44.621Z",
 "op_settings": [0.0007, -0.0004, 100.0], "sensors": {"s1": 518.67, "s2": 642.1, "...": "...", "s21": 23.4}}
```

| Field / choice | Why |
|---|---|
| `event_id = uuid5(engine_id + cycle)` | **Deterministic**: the same reading always gets the same ID. If it is sent twice, both copies have the same ID, so duplicates can be found anywhere downstream. A random ID per send would make a duplicate look like a new event: the most common reason "exactly-once" pipelines are not |
| `engine_id` (used as the Kafka key) | Same key → same partition → each engine's readings stay in order |
| `event_time` set by the producer, in UTC with `Z` | Processing uses **when it happened**, not when it arrived, so late data lands in the right place. UTC avoids time-zone bugs |
| `schema_version` | Lets the format change later without breaking readers |
| `sensors` as a nested object | Keeps the 21 readings together; Spark parses it into columns in silver |

**Tech box: JSON for events.** *Why:* readable, every tool understands it, easy to debug in Kafka UI. *Alternatives:* **Avro/Protobuf** with a schema registry (binary, smaller, schema checked on every send, the production choice at scale). *Impact:* JSON is ~900 bytes per event here; Avro would be several times smaller. The capacity plan mentions switching at large scale.

### Part 2: validation (`validate`)

Returns `None` for a good event, or a **reason code**:
- `MALFORMED_JSON`: not a JSON object at all (for example a truncated message).
- `MISSING_FIELD`: a required field or any sensor is missing or NaN.
- `OUT_OF_RANGE`: a physically impossible value (for example pressure `s11` outside 40-60).

**Why reason codes instead of just "bad"?** You can count them, chart them and route them: "154 missing fields from one producer" tells you where to look. The same rules are written again in Spark in silver (chapter 11) and a test proves both agree.

### Part 3: the health score (`health_index`), in plain words

For each of the 10 drifting sensors:
1. Take the reading's distance from that sensor's **healthy average**, divided by its **healthy spread** (standard deviation). This is a **z-score**: "how many normal-wobbles away from healthy is it?". The healthy average and spread were measured from the first 30 cycles of every FD001 engine (the numbers in `HEALTH_SENSORS`).
2. Multiply by the sensor's **direction** (+1 if it rises with wear, -1 if it falls), so wear always pushes the number **up**.
3. Average the 10 values.

Result: about **0 for a healthy engine**, rising as it wears.

**Why two patterns (HPC and FAN)?** FD003 has a second kind of failure (the fan). In a fan failure, five sensors move in the **opposite** direction to a compressor failure. The first version used only the compressor pattern, so for fan failures the sensors cancelled out and **44 of 100 FD003 engines never alerted**. The fixed version computes both patterns and takes the worse one: `max(hpc, 0.4 × fan)`. The fan pattern is scaled by 0.4 because its sensors move very far (20+ standard deviations) and would otherwise alarm too early. `fault_mode` says which pattern won, so an alert can go to the right team. (Chapter 7 shows how this was measured.)

### Part 4: smoothing and alert levels (`advance_state`)

- One reading is noisy, so the score is smoothed with an **EWMA**: `ewma = 0.2 × new + 0.8 × previous`. It needs only **one number per engine** as memory, which is ideal for streaming (a rolling average of the last 20 readings would need 20 numbers per engine).
- **WARNING** when the EWMA reaches 1.5, **CRITICAL** at 2.5 (calibrated in chapter 7).
- **Levels only go up** until maintenance: an engine does not flap between OK and WARNING.
- An alert is created **only when the level rises**, with ID `engine:LEVEL`, so each alert happens once.
- **Idempotency rule:** events with a cycle **at or before** the last applied cycle are ignored. If Spark re-runs a batch after a crash, nothing changes. Out-of-order events within a batch are sorted by cycle first.

**Check:** you can explain why `event_id` must be deterministic, and what happens in `advance_state` when the same batch is applied twice.

---

<a id="tests"></a>

# 6. Unit tests first

### Tech box: pytest
- **What:** Python's standard test runner. Functions named `test_*` with `assert` statements.
- **Why here:** the rules in `events.py` decide whether alerts are right. Testing them without Kafka or Spark takes a fraction of a second, so you run them after every change.
- **Alternatives:** `unittest` (built in, more boilerplate), testing only end to end (slow, and when it fails you do not know which rule broke).
- **Impact:** the "replay is idempotent" test is your proof that a crash cannot double-count, written as code.

Create `tests/test_events.py`:

```python
"""Fast unit tests of the pure domain logic (no Spark, no Kafka)."""
import json
from datetime import datetime, timezone

import pytest

from telemetry import cmapss
from telemetry.events import (CRITICAL_AT, WARNING_AT, advance_state, event_id, health_index, make_event, to_bytes, validate)

ROW = cmapss.synthetic("FD001", engines=1)[0]
NOW = datetime(2026, 11, 25, 8, 0, tzinfo=timezone.utc)


def ev(cycle, hi, engine="FD001-001"):
    return {"engine_id": engine, "fleet": "FD001", "cycle": cycle, "event_time": f"t{cycle}", "health_index": hi, "s11": 47.5, "s4": 1405.0}


def test_event_id_is_deterministic():
    assert event_id("FD001-001", 5) == event_id("FD001-001", 5)
    assert event_id("FD001-001", 5) != event_id("FD001-001", 6)


def test_make_event_roundtrip_is_valid():
    e = make_event("FD001", ROW, now=NOW)
    assert e["engine_id"] == "FD001-001" and e["event_time"] == "2026-11-25T08:00:00.000Z"
    assert validate(json.loads(to_bytes(e))) is None


@pytest.mark.parametrize("mutate,reason", [
    (lambda e: None, "MALFORMED_JSON"),
    (lambda e: {k: v for k, v in e.items() if k != "engine_id"}, "MISSING_FIELD"),
    (lambda e: {**e, "sensors": {**e["sensors"], "s11": -1.0}}, "OUT_OF_RANGE"),
])
def test_validate_reasons(mutate, reason):
    assert validate(mutate(make_event("FD001", ROW, now=NOW))) == reason


def test_health_index_near_zero_when_healthy():
    assert abs(health_index(ROW)) < 1.5


def test_state_escalates_and_raises_each_alert_once():
    events = [ev(1, 0.0), ev(2, 0.0), ev(3, 10.0), ev(4, 10.0), ev(5, 10.0), ev(6, 10.0)]
    state, alerts = advance_state(None, events)
    assert [a["level"] for a in alerts] == ["WARNING", "CRITICAL"]
    assert state["alert_level"] == 2 and state["last_cycle"] == 6
    assert state["ewma_health"] >= CRITICAL_AT > WARNING_AT


def test_replaying_a_batch_is_idempotent():
    s1, a1 = advance_state(None, [ev(1, 0.0), ev(2, 9.0), ev(3, 9.0)])
    s2, a2 = advance_state(s1, [ev(1, 0.0), ev(2, 9.0), ev(3, 9.0)])   # same batch again after a crash
    assert s2 == s1 and a2 == []


def test_out_of_order_events_are_applied_in_cycle_order_and_stale_ones_ignored():
    s, _ = advance_state(None, [ev(3, 1.0), ev(1, 0.0), ev(2, 0.5)])
    assert s["last_cycle"] == 3
    s2, _ = advance_state(s, [ev(2, 99.0)])                            # stale cycle arrives late
    assert s2 == s


def test_synthetic_engines_degrade():
    rows = [r for r in cmapss.synthetic("FD001", engines=1)]
    assert health_index(rows[-1]) > health_index(rows[0]) + 2


def _shift(pattern_signs, z):
    """A healthy row with every health sensor moved z standard deviations in the given pattern's direction."""
    from telemetry.events import HEALTH_SENSORS
    row = dict(ROW)
    for s, (_, mu, sd) in HEALTH_SENSORS.items():
        row[s] = mu + pattern_signs[s] * z * sd
    return row


def test_fan_fault_is_detected_and_labelled():
    # v1 bug (build-log F14): the fan pattern cancelled itself out in the HPC-only index.
    from telemetry.events import FAN_SIGNS, fault_mode, hpc_index
    fan_row = _shift(FAN_SIGNS, 8.0)
    assert hpc_index(fan_row) < WARNING_AT          # the old index would miss it
    assert health_index(fan_row) >= CRITICAL_AT     # the new one does not
    assert fault_mode(fan_row) == "FAN"


def test_hpc_fault_is_labelled_hpc():
    from telemetry.events import HPC_SIGNS, fault_mode
    hpc_row = _shift(HPC_SIGNS, 4.0)
    assert health_index(hpc_row) >= CRITICAL_AT and fault_mode(hpc_row) == "HPC"


def test_alert_carries_fault_mode():
    events = [{**ev(c, 10.0), "fault_mode": "FAN"} for c in range(1, 6)]
    _, alerts = advance_state(None, events)
    assert {a["fault_mode"] for a in alerts} == {"FAN"}
```

**What each test protects:**

| Test | Protects against |
|---|---|
| `test_event_id_is_deterministic` | Random IDs (would make duplicates undetectable) |
| `test_make_event_roundtrip_is_valid` | The producer writing events the validator rejects |
| `test_validate_reasons` (3 cases, `parametrize`) | Each bad-data type mapping to the right reason code |
| `test_health_index_near_zero_when_healthy` | A broken baseline that alarms on healthy engines |
| `test_state_escalates_and_raises_each_alert_once` | Missing or repeated alerts |
| `test_replaying_a_batch_is_idempotent` | **Double counting after a crash** |
| `test_out_of_order_events...` | Late, out-of-order readings moving an engine backwards |
| `test_fan_fault_is_detected_and_labelled` | The FD003 bug coming back |

Create `tests/test_spark_parity.py` too, but **do not run it yet**: it imports the silver job you write in chapter 12, so it fails until then. It proves the Spark and Python health formulas agree (chapter 12 explains why that matters). In CI, where Spark is not installed, it is skipped automatically.

```python
"""The silver job computes the health index in Spark; gold and the unit tests use the Python version.
They must agree exactly, or alerts would differ from what was calibrated. Skipped if PySpark is not installed."""
import pytest

pyspark = pytest.importorskip("pyspark")

from telemetry import cmapss  # noqa: E402
from telemetry.events import FAN_SIGNS, HEALTH_SENSORS, HPC_SIGNS, fault_mode, health_index  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = SparkSession.builder.master("local[1]").appName("parity").config("spark.ui.enabled", "false").getOrCreate()
    yield s
    s.stop()


def test_spark_and_python_health_index_agree(spark):
    from telemetry.jobs.silver import fault_mode_expr, health_expr
    rows = cmapss.synthetic("FD001", engines=2)[:50]
    for signs, z in ((FAN_SIGNS, 8.0), (HPC_SIGNS, 4.0)):
        r = dict(rows[0])
        for s, (_, mu, sd) in HEALTH_SENSORS.items():
            r[s] = mu + signs[s] * z * sd
        rows.append(r)
    df = spark.createDataFrame([{k: float(v) for k, v in r.items() if k.startswith("s")} for r in rows])
    got = df.select(health_expr().alias("hi"), fault_mode_expr().alias("fm")).collect()
    for r, g in zip(rows, got):
        assert g.hi == pytest.approx(health_index(r), abs=1e-9)
        assert g.fm == fault_mode(r)
```

Run:

```bash
python -m pytest -q tests/test_events.py
```

**Check:** all tests in `test_events.py` pass.

---

<a id="calibrate"></a>

# 7. Calibrate the alerts on real failures

Before streaming anything, find out whether the health score and thresholds actually warn **early** and **only when needed**. This is done offline on all 200 engines, cycle by cycle, using the **same functions** the stream will use.

Create `scripts/calibrate.py`:

```python
"""Offline alert calibration on NASA C-MAPSS run-to-failure data (Milestone T1).

Replays every engine cycle by cycle through the SAME EWMA + threshold logic the stream uses, and reports for each
health-index design: how many engines get a WARNING / CRITICAL before failure, how far ahead (lead time in cycles),
and how many warnings are "too early" (> EARLY cycles before failure, i.e. while the engine is still healthy).

    python scripts/calibrate.py            # compare designs on FD001 + FD003
"""
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from telemetry import events  # noqa: E402
from telemetry.cmapss import load  # noqa: E402

EARLY = 125  # C-MAPSS engines are usually treated as healthy until ~125 cycles before failure


def old_index(s):
    """v1: one signed index tuned to the HPC fault (FD001)."""
    return sum(sign * (s[k] - mu) / sd for k, (sign, mu, sd) in events.HEALTH_SENSORS.items()) / len(events.HEALTH_SENSORS)


def abs_index(s):
    """Alternative: mean absolute deviation (direction-agnostic)."""
    return sum(abs(s[k] - mu) / sd for k, (_, mu, sd) in events.HEALTH_SENSORS.items()) / len(events.HEALTH_SENSORS)


DESIGNS = {
    "v1 signed HPC index (old)": (old_index, 1.5, 2.5),
    "abs deviation": (abs_index, 2.0, 3.0),
    "v2 max(HPC, 0.4 x fan) (new)": (events.health_index, events.WARNING_AT, events.CRITICAL_AT),
}


def replay(rows, index, warn, crit, alpha=events.EWMA_ALPHA):
    by_unit = defaultdict(list)
    for r in rows:
        by_unit[r["unit"]].append(r)
    out = []
    for unit, rs in by_unit.items():
        rs.sort(key=lambda r: r["cycle"])
        last = rs[-1]["cycle"]
        ewma, w, c = None, None, None
        for r in rs:
            hi = index(r)
            ewma = hi if ewma is None else alpha * hi + (1 - alpha) * ewma
            if w is None and ewma >= warn:
                w = last - r["cycle"]
            if c is None and ewma >= crit:
                c = last - r["cycle"]
        out.append((unit, w, c))
    return out


def summarise(res):
    n = len(res)
    wl = [w for _, w, _ in res if w is not None]
    cl = [c for _, _, c in res if c is not None]
    p10 = lambda xs: sorted(xs)[max(0, len(xs) // 10 - 1)] if xs else None  # noqa: E731
    return {
        "engines": n,
        "warned": f"{len(wl)}/{n} ({100 * len(wl) // n}%)",
        "critical": f"{len(cl)}/{n} ({100 * len(cl) // n}%)",
        "warn lead median/p10": f"{st.median(wl):.0f} / {p10(wl)}" if wl else "-",
        "crit lead median/p10": f"{st.median(cl):.0f} / {p10(cl)}" if cl else "-",
        f"too early (>{EARLY})": sum(1 for w in wl if w > EARLY),
    }


def main():
    data = {f: load(f) for f in ("FD001", "FD003")}
    cols = None
    print(f"| Design | Fleet | Engines | Warned | Critical | Warning lead median / p10 | Critical lead median / p10 | Warnings > {EARLY} cycles early |")
    print("|---|---|---|---|---|---|---|---|")
    for name, (idx, warn, crit) in DESIGNS.items():
        for fleet, rows in data.items():
            s = summarise(replay(rows, idx, warn, crit))
            print(f"| {name} | {fleet} | " + " | ".join(str(v) for v in s.values()) + " |")


if __name__ == "__main__":
    main()
```

```bash
python scripts/calibrate.py
```

**How to read the output:**

| Column | Meaning | Good value |
|---|---|---|
| Warned | Engines that got a WARNING before failing | 100% |
| Warning lead median / p10 | Cycles of warning before failure: typical, and the worst 10% | High enough to plan maintenance (~44 median, 25 for the worst 10%) |
| Critical lead | Same for CRITICAL | ~17-18 |
| Warnings > 125 cycles early | Alarms while the engine is still healthy (false alarms) | Close to 0 |

**What you should see:** the old v1 index catches only **56% of FD003**; v2 catches **100% of both fleets** with the same lead time and no extra false alarms.

**Why this matters (the interview story):** a model tuned on one population (FD001) silently failed on another (FD003). It was found only because the evaluation was split **per fleet** instead of one overall number. The fan weight (0.4) was tuned on the odd-numbered FD003 engines and checked on the even-numbered ones, so the result is not "tuned on the test". That is a data-science-quality habit, applied by a data engineer.

**Why α = 0.2 and 1.5 / 2.5?** Several combinations were tried. Lower thresholds warn earlier but alarm more on healthy engines; higher ones warn too late. α = 0.2 with 1.5 / 2.5 caught every engine with ~44 cycles of warning. Try changing `EWMA_ALPHA` or `WARNING_AT` in `events.py` and re-run to see the trade-off yourself.

**Check:** the v2 rows show 100/100 warned for FD001 and FD003.

---

<a id="topic"></a>

# 8. Create the Kafka topic and the command line

Create `telemetry/admin.py`:

```python
"""Kafka topic admin: create / describe / delete the telemetry topic."""
from confluent_kafka.admin import AdminClient, NewTopic

from .config import BOOTSTRAP, PARTITIONS, TOPIC


def client():
    return AdminClient({"bootstrap.servers": BOOTSTRAP})


def create_topic(partitions: int = PARTITIONS, retention_hours: int = 168):
    admin = client()   # keep a reference until the futures resolve
    fut = admin.create_topics([NewTopic(TOPIC, num_partitions=partitions, replication_factor=1,
                                           config={"retention.ms": str(retention_hours * 3600 * 1000), "compression.type": "lz4"})])
    for name, f in fut.items():
        try:
            f.result()
            print(f"created topic {name} with {partitions} partitions, retention {retention_hours}h")
        except Exception as e:  # already exists, etc.
            print(f"{name}: {e}")


def delete_topic():
    admin = client()
    for name, f in admin.delete_topics([TOPIC]).items():
        try:
            f.result()
            print(f"deleted {name}")
        except Exception as e:
            print(f"{name}: {e}")


def describe():
    md = client().list_topics(timeout=10)
    t = md.topics.get(TOPIC)
    if not t:
        print(f"{TOPIC} does not exist")
        return
    print(f"{TOPIC}: {len(t.partitions)} partitions")
    for p in sorted(t.partitions.values(), key=lambda x: x.id):
        print(f"  partition {p.id} leader={p.leader} replicas={p.replicas}")
```

| Choice | Why |
|---|---|
| **6 partitions** | Up to 6 consumers (Spark tasks) can read in parallel. Choose partitions for future parallelism: increasing them later changes which partition a key goes to and breaks per-engine ordering |
| **replication_factor=1** | Only one broker locally. Production: 3, so a broker can die without data loss |
| **retention 168 h** | Messages kept 7 days: you can replay a week after a bug |
| **lz4 compression** | Fast compression; less disk and network |
| `admin = client()` kept in a variable | The client must live until the request finishes. The first version did `client().create_topics(...)`: Python deleted the client too early and the topic was silently not created (F5) |

Create `telemetry/cli.py`, one entry point for every command:

```python
"""Command line entry point:  python -m telemetry.cli <command> [options]"""
import argparse
import shutil

from . import admin, cmapss
from .config import DATA_DIR, LAKE


def main(argv=None):
    p = argparse.ArgumentParser(prog="telemetry", description="Turbofan Telemetry Streaming Platform")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("download", help="download the NASA C-MAPSS dataset into data/cmapss")
    t = sub.add_parser("create-topic", help="create the Kafka topic")
    t.add_argument("--partitions", type=int, default=6)
    sub.add_parser("describe-topic")
    sub.add_parser("delete-topic")
    pr = sub.add_parser("produce", help="replay engines into Kafka")
    pr.add_argument("--rate", type=float, default=500, help="events/second, 0 = as fast as possible")
    pr.add_argument("--engines", type=int, default=None, help="limit engines per subset")
    pr.add_argument("--loops", type=int, default=1, help="replay the fleet N times (new engine ids each loop)")
    pr.add_argument("--start-loop", type=int, default=0, help="first loop number; use a new value for each run so engine ids never repeat")
    pr.add_argument("--max-events", type=int, default=None)
    pr.add_argument("--dup-rate", type=float, default=0.0)
    pr.add_argument("--late-rate", type=float, default=0.0)
    pr.add_argument("--late-seconds", type=float, default=30.0)
    pr.add_argument("--bad-rate", type=float, default=0.0)
    pr.add_argument("--chaos", action="store_true", help="shortcut: dup 5%%, late 3%% (30 s), bad 1%%")
    pr.add_argument("--no-manifest", action="store_true", help="don't record event ids (use for big load tests)")
    st = sub.add_parser("stream", help="run streaming layers")
    st.add_argument("layers", nargs="*", choices=["bronze", "silver", "gold"], help="default: all three")
    sub.add_parser("report", help="print pipeline health: counts, duplicates, dead letters, latency, alerts")
    v = sub.add_parser("verify", help="exactly-once check: producer manifest vs silver")
    v.add_argument("--run", default=None)
    sub.add_parser("reset", help="delete the local lake (tables + checkpoints) and manifests")
    a = p.parse_args(argv)

    if a.cmd == "download":
        cmapss.download()
    elif a.cmd == "create-topic":
        admin.create_topic(a.partitions)
    elif a.cmd == "describe-topic":
        admin.describe()
    elif a.cmd == "delete-topic":
        admin.delete_topic()
    elif a.cmd == "produce":
        from . import producer
        dup, late, bad = (0.05, 0.03, 0.01) if a.chaos else (a.dup_rate, a.late_rate, a.bad_rate)
        producer.run(rate=a.rate, engines=a.engines, loops=a.loops, start_loop=a.start_loop, max_events=a.max_events, dup_rate=dup,
                     late_rate=late, late_seconds=a.late_seconds, bad_rate=bad, manifest=not a.no_manifest)
    elif a.cmd == "stream":
        from .jobs import run
        run.main(tuple(a.layers or ("bronze", "silver", "gold")))
    elif a.cmd == "report":
        from . import report
        report.report()
    elif a.cmd == "verify":
        from . import report
        raise SystemExit(0 if report.verify(a.run) else 1)
    elif a.cmd == "reset":
        shutil.rmtree(LAKE, ignore_errors=True)
        shutil.rmtree(DATA_DIR / "manifests", ignore_errors=True)
        print(f"removed {LAKE} and producer manifests (Kafka topic untouched: delete-topic + create-topic to clear it)")


if __name__ == "__main__":
    main()
```

**Why one CLI?** The same command runs the same way on your laptop, in Docker, in a script and from an orchestrator. Notice the `stream` command has **no list default** in argparse: the first version had one and argparse rejected its own default (F6).

```bash
python -m telemetry.cli create-topic --partitions 6
python -m telemetry.cli describe-topic
```

**Check:** `describe-topic` lists 6 partitions, and Kafka UI (http://localhost:8080) shows the topic `engine-telemetry`.

---

<a id="producer"></a>

# 9. The producer: replay the fleet, with chaos

Create `telemetry/producer.py`:

```python
"""Replay C-MAPSS engines into Kafka as a live stream, with optional chaos.

Every "tick", each active engine emits its next cycle, so many engines stream in
parallel like a real fleet. The message key is engine_id, so all events of one engine
land in the same partition and stay in order.

Chaos (for testing the pipeline's guarantees):
  --dup-rate    resend some events (at-least-once delivery duplicates)
  --late-rate   hold some events back for --late-seconds before sending (late arrivals)
  --bad-rate    send malformed / incomplete / out-of-range events (must go to the dead-letter table)
"""
import heapq
import json
import random
import time
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone

from confluent_kafka import Producer

from . import cmapss
from .config import BOOTSTRAP, DATA_DIR, TOPIC
from .events import make_event, to_bytes


def fleet_stream(subsets, loops, engines_limit=None, start_loop=0):
    """Yield (subset, row, loop) interleaved across engines: one cycle per engine per tick.
    Each loop replays the fleet under new engine ids (FD001-007-L1, -L2, ...), so a new run never
    re-sends an old event_id (silver only de-duplicates inside the watermark window)."""
    for loop in range(start_loop, start_loop + loops):
        iters = []
        for sub in subsets:
            by_unit = defaultdict(list)
            for r in cmapss.load(sub):
                by_unit[r["unit"]].append(r)
            units = sorted(by_unit)[:engines_limit] if engines_limit else sorted(by_unit)
            iters += [(sub, iter(sorted(by_unit[u], key=lambda r: r["cycle"]))) for u in units]
        while iters:
            alive = []
            for sub, it in iters:
                row = next(it, None)
                if row is not None:
                    yield sub, row, loop
                    alive.append((sub, it))
            iters = alive


def _bad_event(rnd, good: dict) -> tuple[bytes, str]:
    kind = rnd.choice(["MALFORMED_JSON", "MISSING_FIELD", "OUT_OF_RANGE"])
    if kind == "MALFORMED_JSON":
        return to_bytes(good)[: rnd.randint(5, 60)], kind          # truncated message
    bad = json.loads(to_bytes(good))
    bad["event_id"] = str(uuid.uuid4())                            # never counts as a valid event
    if kind == "MISSING_FIELD":
        bad.pop(rnd.choice(["engine_id", "cycle", "event_time"]))
    else:
        bad["sensors"]["s11"] = -1.0                               # physically impossible pressure
    return to_bytes(bad), kind


def run(rate: float = 500, subsets=cmapss.DEFAULT_SUBSETS, loops: int = 1, engines: int | None = None, start_loop: int = 0,
        dup_rate: float = 0.0, late_rate: float = 0.0, late_seconds: float = 30.0, bad_rate: float = 0.0,
        max_events: int | None = None, manifest: bool = True, seed: int = 42) -> dict:
    rnd = random.Random(seed)
    run_id = datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%SZ")
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",                 # wait for all in-sync replicas
        "enable.idempotence": True,    # broker drops producer retries: no duplicates from the producer itself
        "linger.ms": 20,               # small batching delay -> much higher throughput
        "compression.type": "lz4",
        "queue.buffering.max.messages": 500000,
    })
    errors = Counter()

    def on_delivery(err, msg):
        if err:
            errors[str(err)] += 1

    stats = Counter()
    valid_ids = []
    delayed = []  # heap of (send_at, seq, key, payload)
    seq = 0
    start = time.time()
    sent = 0

    def send(key: str, payload: bytes):
        nonlocal sent
        while True:
            try:
                producer.produce(TOPIC, key=key, value=payload, on_delivery=on_delivery)
                break
            except BufferError:          # local queue full: let it drain (backpressure)
                producer.poll(0.05)
        sent += 1
        producer.poll(0)

    print(f"[{run_id}] producing to {TOPIC} @ {BOOTSTRAP} rate={rate or 'max'}/s dup={dup_rate} late={late_rate} bad={bad_rate}")
    for sub, row, loop in fleet_stream(subsets, loops, engines, start_loop):
        if max_events and stats["events"] >= max_events:
            break
        now = time.time()
        while delayed and delayed[0][0] <= now:            # release late events whose delay is over
            _, _, k, p = heapq.heappop(delayed)
            send(k, p)
        ev = make_event(sub, row, loop)
        payload = to_bytes(ev)
        stats["events"] += 1
        if rnd.random() < bad_rate:
            bad, kind = _bad_event(rnd, ev)
            send(ev["engine_id"], bad)
            stats[f"bad_{kind}"] += 1
            continue
        valid_ids.append(ev["event_id"]) if manifest else None
        stats["valid_unique"] += 1
        if rnd.random() < late_rate:
            seq += 1
            heapq.heappush(delayed, (now + late_seconds, seq, ev["engine_id"], payload))
            stats["late"] += 1
        else:
            send(ev["engine_id"], payload)
        if rnd.random() < dup_rate:
            send(ev["engine_id"], payload)
            stats["duplicates_sent"] += 1
        if rate:
            delay = start + stats["events"] / rate - time.time()   # read the clock ONCE (see build-log F15)
            if delay > 0:
                time.sleep(delay)
        if stats["events"] % 20000 == 0:
            el = time.time() - start
            print(f"  {stats['events']:,} events in {el:,.1f}s ({stats['events'] / el:,.0f}/s)")
    while delayed:
        wait = delayed[0][0] - time.time()
        if wait > 0:
            time.sleep(min(wait, 1))
            continue
        _, _, k, p = heapq.heappop(delayed)
        send(k, p)
    producer.flush(60)
    elapsed = time.time() - start
    summary = {"run_id": run_id, "topic": TOPIC, "messages_sent": sent, "elapsed_s": round(elapsed, 2),
               "throughput_msgs_per_s": round(sent / elapsed, 1) if elapsed else None, "delivery_errors": dict(errors), **stats}
    if manifest:
        mdir = DATA_DIR / "manifests"
        mdir.mkdir(parents=True, exist_ok=True)
        (mdir / f"{run_id}.ids").write_text("\n".join(valid_ids))
        (mdir / f"{run_id}.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    return summary
```

### How it works
- **`fleet_stream`** interleaves engines: each "tick", every engine still alive sends its next cycle. So you see 200 engines streaming at once, like a real fleet, instead of engine 1's whole life and then engine 2's.
- **`--loops` / `--start-loop`** replay the fleet under **new engine IDs** (`FD001-007-L1`). Silver only remembers event IDs for 10 minutes, so a second run reusing old IDs 15 minutes later would land as duplicates (F13). Each run gets fresh IDs.
- **Rate control:** `--rate 500` = 500 events per second. The clock is read **once** per step: the first version read it twice, and at high rates the sleep became negative and crashed the producer (F15).
- **`BufferError` handling:** if the local send queue is full, wait for it to drain. That is backpressure on the producer side.
- **Late events** go into a heap (a priority queue) and are released after `--late-seconds`.

### The producer settings (memorise these)

| Setting | Meaning | Why |
|---|---|---|
| `acks=all` | The broker confirms only when all in-sync replicas have the message | No acknowledged message is lost |
| `enable.idempotence=True` | The broker drops the producer's own retry duplicates | Network retries cannot create duplicates |
| `linger.ms=20` | Wait up to 20 ms to fill a batch | Batching = far higher throughput (~28,000 events/s measured) |
| `compression.type=lz4` | Compress batches | Less network and disk |
| `on_delivery` callback | Counts failed sends | A send you never check is a send you cannot trust |
| `flush(60)` at the end | Wait for everything to be delivered | Without it, queued messages are lost when the program exits |

### Chaos: why break your own pipeline?
`--chaos` = 5% duplicates, 3% late by 30 s, 1% bad messages (truncated JSON, a missing field, or impossible pressure `s11 = -1`). Real sources do all of these. If you test only with clean data, your "exactly-once" claim has never been tested.

### The manifest: how exactly-once becomes provable
Each run writes `data/manifests/<run>.ids`, the IDs of every **valid** event it intended to send. Later, `verify` compares this list with what is in silver: missing, extra, duplicates. Ground truth comes from outside the pipeline, so the pipeline cannot "grade its own homework".

### Try it

```bash
python -m telemetry.cli produce --rate 200 --engines 5 --max-events 500
```

Open Kafka UI → Topics → engine-telemetry → Messages. You will see JSON events, the key is the engine ID, and each engine stays on one partition.

**Check:** the producer prints a summary with `messages_sent` and an empty `delivery_errors`, and the messages are visible in Kafka UI.

---

<a id="spark"></a>

# 10. The Spark session

### Tech box: Apache Spark and Structured Streaming
- **What it is:** a distributed processing engine. You describe what to do with DataFrames (like tables); Spark plans it and runs it across cores or machines. **Structured Streaming** runs the same DataFrame code continuously on new data, in micro-batches.
- **Why here:** the same API is used for batch and streaming, it reads Kafka and writes Delta natively, it has **checkpoints** for crash recovery, and it is what Databricks (and most Cummins-style data platforms) run.
- **Alternatives:** **Apache Flink** (true event-at-a-time streaming, lower latency, separate skill set), **Kafka Streams** (Java library, Kafka-to-Kafka only), plain Python consumers (you would have to build checkpoints, parallelism, state and exactly-once yourself).
- **Advantages:** one skill for batch and stream, mature exactly-once with Delta, scales from a laptop (`local[*]`) to a cluster without code changes.
- **Impact:** micro-batches add a few seconds of latency. For engine health that changes once per flight, that is fine; for sub-second use cases Flink would be considered.

### Tech box: Delta Lake
- **What it is:** a table format: Parquet data files + a `_delta_log` folder of JSON commit files. A file belongs to the table only once a commit adds it.
- **Why here:** plain Parquet folders break when a job dies mid-write (half-written files are read as data). Delta commits are **atomic**: readers see all of a write or none of it. It also gives **MERGE** (upserts, needed for gold), schema enforcement, time travel and a history of every commit.
- **Alternatives:** Apache Iceberg (similar, engine-neutral), Apache Hudi (upsert-heavy streaming), plain Parquet (no transactions).
- **Impact:** Spark checkpoints + Delta's atomic commits are what make the pipeline **exactly-once into tables**.

Create `telemetry/spark.py`:

```python
"""SparkSession with Delta Lake and the Kafka connector (jars are downloaded from Maven on first run)."""
import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"


def get_spark(app: str = "turbofan-telemetry") -> SparkSession:
    # Python workers must use this same interpreter/venv (pandas + pyarrow for applyInPandas)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = (SparkSession.builder.appName(app)
               .master(os.environ.get("SPARK_MASTER", "local[*]"))
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.shuffle.partitions", os.environ.get("TT_SHUFFLE_PARTITIONS", "8"))
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.streaming.stateStore.providerClass",
                       "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
               .config("spark.driver.memory", os.environ.get("TT_DRIVER_MEMORY", "3g"))
               .config("spark.ui.showConsoleProgress", "false"))
    spark = configure_spark_with_delta_pip(builder, extra_packages=[KAFKA_PKG]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
```

| Line | What and why |
|---|---|
| `PYSPARK_PYTHON = sys.executable` | Spark starts **separate Python worker processes** for pandas code (gold). They must use this venv, or they cannot find `pyarrow` and gold crashes (F9: this exact bug happened) |
| `master("local[*]")` | Run Spark inside this one process using all cores. On a cluster you change only this |
| Delta `spark.sql.extensions` + `catalog` | Turn on Delta SQL and MERGE |
| `shuffle.partitions = 8` | Spark's default is 200, far too many for a laptop (200 tiny tasks per shuffle). Roughly 2× your cores |
| `session.timeZone = UTC` | Timestamps parse and display the same on every machine |
| `RocksDBStateStoreProvider` | Streaming state (dedupe IDs, windows) kept in RocksDB on disk instead of the Java heap: steady memory, the production choice |
| `driver.memory 3g` | Enough for local runs |
| `configure_spark_with_delta_pip(..., extra_packages=[KAFKA_PKG])` | Downloads the Delta and **Kafka connector** jars from Maven Central on first start (about 1 minute, once). The connector version (`_2.13:4.0.1`) must match Spark and Scala versions exactly |

First start may print `... repos.spark-packages.org ... 403`: harmless, Spark tries several download sites (F8).

**Check:** `python -c "from telemetry.spark import get_spark; s = get_spark(); print(s.version); s.stop()"` prints `4.0.1`.

---

<a id="bronze"></a>

# 11. Bronze: store exactly what arrived

Create `telemetry/jobs/bronze.py`:

```python
"""Bronze: Kafka -> Delta, exactly as received (plus Kafka coordinates and ingest time).

Nothing is parsed or dropped here. Bronze is the replayable record of what arrived.
"""
from pyspark.sql import functions as F

from ..config import BOOTSTRAP, CHECKPOINTS, MAX_OFFSETS_PER_TRIGGER, TABLES, TOPIC, TRIGGER


def start(spark):
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)   # backpressure: cap each micro-batch
           .load())
    out = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_value"),
        "topic", "partition", "offset",
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingest_ts"),
    ).withColumn("ingest_date", F.to_date("ingest_ts"))
    return (out.writeStream.format("delta").queryName("bronze")
            .outputMode("append")
            .option("checkpointLocation", str(CHECKPOINTS / "bronze"))   # offsets + commit log = exactly-once into Delta
            .partitionBy("ingest_date")
            .trigger(processingTime=TRIGGER)
            .start(str(TABLES["bronze"])))
```

**What it does:** reads the Kafka topic as a stream and writes every message to a Delta table **as received**: the key and value as strings, plus Kafka's coordinates (`topic, partition, offset, kafka_ts`) and the time we ingested it.

| Choice | Why |
|---|---|
| **Nothing parsed, nothing dropped** | Bronze is the **replayable record**. If silver has a bug, fix it and rebuild silver from bronze. Never "clean" in bronze |
| `startingOffsets = earliest` | On the very first start, read everything in the topic. Afterwards the checkpoint decides |
| `maxOffsetsPerTrigger` | Backpressure: each batch takes at most 200k messages |
| `checkpointLocation` | Spark writes "batch N = offsets X..Y" **before** processing and "batch N done" **after** the Delta commit. After a crash it redoes only unfinished batches, and Delta ignores a batch already committed. Result: **exactly-once into bronze** |
| `partitionBy("ingest_date")` | One folder per day: old days can be dropped cheaply; queries for a day read only that folder |
| `outputMode("append")` | Bronze only ever adds rows |

### Tech box: the medallion architecture (bronze / silver / gold)
- **What:** three layers with clear contracts: raw → cleaned/conformed → business-ready.
- **Why:** each layer has one job, so bugs are easy to localise and fix, and the raw layer makes everything rebuildable.
- **Alternatives:** one big job from Kafka straight to final tables (fast to write, impossible to repair after a bug), or two layers.
- **Impact:** in this project, when the health index changed (v1 → v2), silver could be rebuilt from bronze. That is the point.

---

<a id="silver"></a>

# 12. Silver: validate, dead-letter, deduplicate

Create `telemetry/jobs/silver.py`:

```python
"""Silver: parse, validate, deduplicate. Bad records go to a dead-letter table, never silently dropped.

Two streaming queries read the same bronze table:
  silver      valid events, typed, health index added, de-duplicated on event_id within the watermark
  dead_letter invalid events with the raw payload and a reason code
"""
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..cmapss import SENSORS
from ..config import CHECKPOINTS, TABLES, TRIGGER, WATERMARK
from ..events import FAN_SIGNS, FAN_WEIGHT, HEALTH_SENSORS, HPC_SIGNS, SENSOR_RANGES

EVENT_SCHEMA = T.StructType([
    T.StructField("schema_version", T.IntegerType()),
    T.StructField("event_id", T.StringType()),
    T.StructField("fleet", T.StringType()),
    T.StructField("engine_id", T.StringType()),
    T.StructField("cycle", T.IntegerType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("op_settings", T.ArrayType(T.DoubleType())),
    T.StructField("sensors", T.StructType([T.StructField(s, T.DoubleType()) for s in SENSORS])),
])


def classify(bronze_df):
    """Adds `e` (parsed struct), `event_ts` and `dlq_reason` (NULL = valid). Same rules as events.validate()."""
    e = F.col("e")
    df = (bronze_df.withColumn("e", F.from_json("raw_value", EVENT_SCHEMA))
          .withColumn("event_ts", F.to_timestamp(F.col("e.event_time"))))
    malformed = e.isNull() | (e["event_id"].isNull() & e["engine_id"].isNull() & e["cycle"].isNull() & e["sensors"].isNull())
    missing = reduce(lambda a, b: a | b, [e[f].isNull() for f in ("event_id", "engine_id", "cycle", "event_time", "sensors")]
                     + [F.col("event_ts").isNull()] + [e["sensors"][s].isNull() for s in SENSORS])
    out_of_range = reduce(lambda a, b: a | b, [(e["sensors"][s] < lo) | (e["sensors"][s] > hi) for s, (lo, hi) in SENSOR_RANGES.items()])
    reason = (F.when(malformed, "MALFORMED_JSON").when(missing, "MISSING_FIELD").when(out_of_range, "OUT_OF_RANGE"))
    return df.withColumn("dlq_reason", reason)


def _index_expr(signs):
    terms = [F.lit(float(signs[s])) * (F.col(s) - F.lit(mu)) / F.lit(sd) for s, (_, mu, sd) in HEALTH_SENSORS.items()]
    return reduce(lambda a, b: a + b, terms) / F.lit(float(len(terms)))


def hpc_expr():
    return _index_expr(HPC_SIGNS)


def fan_expr():
    return F.lit(FAN_WEIGHT) * _index_expr(FAN_SIGNS)


def health_expr():
    """Same as events.health_index(): the worse of the HPC and (weighted) fan fault patterns."""
    return F.greatest(hpc_expr(), fan_expr())


def fault_mode_expr():
    return F.when(fan_expr() > hpc_expr(), F.lit("FAN")).otherwise(F.lit("HPC"))


def valid_events(classified):
    v = classified.filter(F.col("dlq_reason").isNull())
    return (v.select(
                F.col("e.event_id").alias("event_id"), F.col("e.fleet").alias("fleet"),
                F.col("e.engine_id").alias("engine_id"), F.col("e.cycle").alias("cycle"),
                F.col("event_ts").alias("event_time"),
                *[F.col("e.sensors")[s].alias(s) for s in SENSORS],
                F.col("e.op_settings").alias("op_settings"),
                "kafka_ts", "ingest_ts", "partition", "offset")
            .withColumn("health_index", health_expr())
            .withColumn("fault_mode", fault_mode_expr())
            .withColumn("event_date", F.to_date("event_time")))


def start(spark):
    bronze = spark.readStream.format("delta").load(str(TABLES["bronze"]))
    classified = classify(bronze)

    silver = (valid_events(classified)
              .withWatermark("event_time", WATERMARK)
              .dropDuplicatesWithinWatermark(["event_id"])          # at-least-once in, exactly-once out
              .withColumn("silver_ts", F.current_timestamp()))
    q_silver = (silver.writeStream.format("delta").queryName("silver")
                .outputMode("append")
                .option("checkpointLocation", str(CHECKPOINTS / "silver"))
                .partitionBy("event_date")
                .trigger(processingTime=TRIGGER)
                .start(str(TABLES["silver"])))

    dlq = (classified.filter(F.col("dlq_reason").isNotNull())
           .select("raw_value", "dlq_reason", "kafka_key", "partition", "offset", "kafka_ts", "ingest_ts",
                   F.current_timestamp().alias("dlq_ts")))
    q_dlq = (dlq.writeStream.format("delta").queryName("dead_letter")
             .outputMode("append")
             .option("checkpointLocation", str(CHECKPOINTS / "dead_letter"))
             .trigger(processingTime=TRIGGER)
             .start(str(TABLES["dlq"])))
    return [q_silver, q_dlq]
```

### Step by step
1. **Parse** `raw_value` with `from_json` and an **explicit schema** (`EVENT_SCHEMA`). Never let Spark guess the schema of streaming JSON: a guess can change from batch to batch.
2. **Classify** every row with `dlq_reason`: `MALFORMED_JSON`, `MISSING_FIELD`, `OUT_OF_RANGE`, or NULL (valid). These are the **same rules** as `events.validate()` in chapter 5.
3. **Two queries from the same bronze stream:**
   - `silver`: valid rows, one column per sensor, plus `health_index` and `fault_mode` computed as Spark expressions.
   - `dead_letter`: invalid rows with the **raw payload and the reason**, so they can be inspected and replayed after a fix.
4. **Deduplicate:** `withWatermark("event_time", "10 minutes").dropDuplicatesWithinWatermark(["event_id"])`.

### Tech box: dead-letter table
- **What:** a table for records that cannot be processed, with why.
- **Why:** one bad message must never stop millions of good ones, and must never vanish silently either.
- **Alternatives:** fail the whole stream on the first bad record (one broken device stops the fleet), or drop bad records (silent data loss).
- **Impact:** the demo sends 438 bad messages; all 438 end up here, split exactly by reason, and none leak into silver.

### Tech box: watermark-based deduplication
- **What:** Spark remembers each `event_id` for 10 minutes of event time (in the RocksDB state store), drops repeats, then forgets old IDs.
- **Why:** duplicates come from producer retries and replays. Remembering every ID forever would grow memory without limit.
- **Trade-off (say this in interviews):** a duplicate arriving **more than 10 minutes** after the original is not caught. Global dedupe would need a MERGE into silver on `event_id` (more expensive) or a longer watermark (more state). This is a deliberate, documented choice.
- **Alternatives:** `dropDuplicates` without a watermark (state grows forever), MERGE-based dedupe (exact but costly).

**Why the health index is computed in Spark here, not in Python:** built-in Spark expressions run inside the JVM and are optimised; a Python UDF would ship every row to Python and back (slow). But now the formula exists twice (Spark and Python). `tests/test_spark_parity.py` proves they give **identical** results. Run it now:

```bash
python -m pytest -q tests/test_spark_parity.py
```

**Check:** the parity test passes.

---

<a id="gold"></a>

# 13. Gold: engine state, alerts and fleet health

Create `telemetry/jobs/gold.py`:

```python
"""Gold: engine current state + idempotent alerts (stateful, via foreachBatch + MERGE), and fleet health per minute.

engine_current_state  one row per engine: last cycle, smoothed health (EWMA), alert level
engine_alerts         one row per (engine, level) the first time the level is reached
fleet_health_minute   1-minute event-time windows per fleet (append mode, closed by the watermark)
"""
import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..config import CHECKPOINTS, TABLES, TRIGGER
from ..events import advance_state

STATE_SCHEMA = T.StructType([
    T.StructField("engine_id", T.StringType()), T.StructField("fleet", T.StringType()),
    T.StructField("last_cycle", T.IntegerType()), T.StructField("last_event_time", T.TimestampType()),
    T.StructField("health_index", T.DoubleType()), T.StructField("ewma_health", T.DoubleType()),
    T.StructField("alert_level", T.IntegerType()), T.StructField("s11", T.DoubleType()), T.StructField("s4", T.DoubleType()),
    T.StructField("fault_mode", T.StringType()),
])
ALERT_SCHEMA = T.StructType([
    T.StructField("alert_id", T.StringType()), T.StructField("engine_id", T.StringType()), T.StructField("fleet", T.StringType()),
    T.StructField("level", T.StringType()), T.StructField("cycle", T.IntegerType()), T.StructField("ewma_health", T.DoubleType()),
    T.StructField("event_time", T.TimestampType()), T.StructField("fault_mode", T.StringType()),
])
# applyInPandas returns one schema, so state rows and alert rows share it with a `kind` column
OUT_SCHEMA = T.StructType([T.StructField("kind", T.StringType())] + STATE_SCHEMA.fields
                          + [T.StructField("alert_id", T.StringType()), T.StructField("level", T.StringType())])
PREV = ["last_cycle", "ewma_health", "alert_level", "fleet", "last_event_time", "health_index", "s11", "s4", "fault_mode"]


def ensure_tables(spark):
    for name, schema in (("engine_state", STATE_SCHEMA), ("alerts", ALERT_SCHEMA)):
        if not DeltaTable.isDeltaTable(spark, str(TABLES[name])):
            spark.createDataFrame([], schema).write.format("delta").save(str(TABLES[name]))


def _fold(pdf: pd.DataFrame) -> pd.DataFrame:
    """All events of ONE engine in this micro-batch (+ its previous state) -> new state row + alert rows."""
    first = pdf.iloc[0]
    prev = None
    if pd.notna(first["prev_last_cycle"]):
        prev = {"engine_id": first["engine_id"], **{c: first[f"prev_{c}"] for c in PREV}}
        prev["last_cycle"], prev["alert_level"] = int(prev["last_cycle"]), int(prev["alert_level"])
    events = pdf[["engine_id", "fleet", "cycle", "event_time", "health_index", "s11", "s4", "fault_mode"]].to_dict("records")
    state, alerts = advance_state(prev, events)
    rows = []
    if state and (prev is None or state["last_cycle"] != prev["last_cycle"]):
        rows.append({"kind": "state", **state})
    for a in alerts:
        rows.append({"kind": "alert", "engine_id": a["engine_id"], "fleet": a["fleet"], "last_cycle": a["cycle"],
                     "ewma_health": a["ewma_health"], "last_event_time": a["event_time"], "alert_id": a["alert_id"], "level": a["level"], "fault_mode": a["fault_mode"]})
    cols = [f.name for f in OUT_SCHEMA.fields]
    return pd.DataFrame(rows, columns=cols)


def upsert_state_and_alerts(batch, batch_id):
    spark = batch.sparkSession
    events = batch.select("engine_id", "fleet", "cycle", "event_time", "health_index", "s11", "s4", "fault_mode")
    prev = spark.read.format("delta").load(str(TABLES["engine_state"])).select(
        "engine_id", *[F.col(c).alias(f"prev_{c}") for c in PREV])
    # IMPORTANT: materialise the result ONCE before writing anything. `out` is computed from the
    # current engine_state table; if Spark lazily recomputed it after we MERGE new state, every
    # event would look "already applied" and alerts would silently vanish (bug found in testing,
    # see docs/build-log.md). localCheckpoint(eager=True) computes it now and cuts the lineage.
    out = (events.join(prev, "engine_id", "left")
           .groupBy("engine_id").applyInPandas(_fold, OUT_SCHEMA)
           .localCheckpoint(eager=True))
    alerts = out.filter("kind = 'alert'").select(
        "alert_id", "engine_id", "fleet", "level", F.col("last_cycle").alias("cycle"), "ewma_health",
        F.col("last_event_time").alias("event_time"), "fault_mode")
    (DeltaTable.forPath(spark, str(TABLES["alerts"])).alias("t")
     .merge(alerts.alias("s"), "t.alert_id = s.alert_id")
     .whenNotMatchedInsertAll()                                            # replaying a batch can't raise an alert twice
     .execute())
    state = out.filter("kind = 'state'").select(*[f.name for f in STATE_SCHEMA.fields])
    (DeltaTable.forPath(spark, str(TABLES["engine_state"])).alias("t")
     .merge(state.alias("s"), "t.engine_id = s.engine_id")
     .whenMatchedUpdateAll(condition="s.last_cycle > t.last_cycle")       # never move an engine backwards
     .whenNotMatchedInsertAll()
     .execute())


def start(spark):
    ensure_tables(spark)
    silver = spark.readStream.format("delta").load(str(TABLES["silver"]))
    q_state = (silver.writeStream.queryName("engine_state")
               .foreachBatch(upsert_state_and_alerts)
               .option("checkpointLocation", str(CHECKPOINTS / "engine_state"))
               .trigger(processingTime=TRIGGER)
               .start())
    fleet = (silver.withWatermark("event_time", "2 minutes")
             .groupBy(F.window("event_time", "1 minute").alias("w"), "fleet")
             .agg(F.count("*").alias("events"),
                  F.approx_count_distinct("engine_id").alias("engines"),
                  F.avg("health_index").alias("avg_health_index"),
                  F.max("health_index").alias("max_health_index"))
             .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "fleet",
                     "events", "engines", "avg_health_index", "max_health_index"))
    q_fleet = (fleet.writeStream.format("delta").queryName("fleet_health_minute")
               .outputMode("append")
               .option("checkpointLocation", str(CHECKPOINTS / "fleet_health_minute"))
               .trigger(processingTime=TRIGGER)
               .start(str(TABLES["fleet_minute"])))
    return [q_state, q_fleet]
```

### Three gold tables

| Table | One row per | Used for |
|---|---|---|
| `engine_current_state` | engine | "Which engines are closest to failure right now?" |
| `engine_alerts` | engine × level (WARNING / CRITICAL) | Notifying maintenance, each alert exactly once |
| `fleet_health_minute` | fleet × minute | Trend charts |

### How the engine state is updated (every micro-batch)
1. Take the batch's silver events and **join** each engine's events with its **previous state** from the `engine_current_state` table.
2. `groupBy("engine_id").applyInPandas(_fold, ...)`: for each engine, run the **same** `advance_state` function as the unit tests (chapter 5): sort by cycle, update the EWMA, escalate the level, create alerts.
3. **`localCheckpoint(eager=True)`**: compute the result **now** and cut the link back to the state table (explained below).
4. **MERGE alerts** on `alert_id` (`engine:LEVEL`), insert only if new.
5. **MERGE state** on `engine_id`, update only if `last_cycle` moved forward.

**Why it is idempotent:** if Spark crashes after writing but before checkpointing, it replays the same batch. Cycles at or before `last_cycle` are ignored and existing alert IDs are not inserted again. A replay changes nothing.

### Tech box: foreachBatch + MERGE (instead of a built-in stateful operator)
- **What:** `foreachBatch` gives you each micro-batch as a normal DataFrame, so you can run any batch logic, including Delta MERGE.
- **Why:** the state lives in a **normal Delta table** that dashboards can query directly, and the logic is a plain Python function shared with tests.
- **Alternatives:** `applyInPandasWithState` / `transformWithState` (state hidden inside the checkpoint, not queryable), or a key-value store for state at very large scale.
- **Trade-off:** foreachBatch alone is at-least-once; correctness comes from the MERGE keys. And this step is the slowest part of the pipeline (chapter 18).

### The subtle bug this code prevents (F10): read this twice
The first version had **156 engines in CRITICAL but 0 alerts**, with no error. Why: Spark DataFrames are **lazy**. `out` is a recipe that includes "read the previous state table". The code wrote the **state first**, then computed alerts from `out`. Spark recomputed `out` from scratch, re-reading a state table that had **just been updated**, so every event looked "already applied" and no alerts came out. `cache()` did not help because caching is a performance hint, not a guarantee.
**Fix:** `localCheckpoint(eager=True)` (compute once, cut the lineage) and write alerts first.
**Lesson:** in Spark, never read and write the same table in one lazy plan without materialising first.

### Fleet health per minute
A normal streaming aggregation: 1-minute **event-time** windows per fleet, 2-minute watermark, **append** mode. A window is written once, when it is final (the watermark has passed its end). If data stops arriving, the watermark stops moving and the last windows are not written yet. That is correct behaviour, not a bug (F11): the demo's second, slower wave moves the watermark forward.

---

<a id="run"></a>

# 14. Run the streaming application

Create `telemetry/jobs/run.py`:

```python
"""Start streaming layers in one Spark application and print throughput every N seconds."""
import json
import threading
import time
from datetime import datetime, timezone

from delta.tables import DeltaTable

from ..config import LAKE, TABLES
from ..spark import get_spark
from . import bronze, gold, silver


def _wait_for(spark, path, timeout=600):
    t0 = time.time()
    while not DeltaTable.isDeltaTable(spark, str(path)):
        if time.time() - t0 > timeout:
            raise TimeoutError(f"{path} never appeared. Is the upstream layer running and receiving data?")
        print(f"  waiting for {path.name} to be created by the upstream query...")
        time.sleep(5)


def _monitor(spark, every):
    log = LAKE / "_metrics" / "progress.jsonl"
    log.parent.mkdir(parents=True, exist_ok=True)
    while True:
        time.sleep(every)
        lines = []
        for q in spark.streams.active:
            p = q.lastProgress
            if not p:
                continue
            rec = {"ts": datetime.now(timezone.utc).isoformat(), "query": q.name, "batch": p["batchId"],
                   "rows": p["numInputRows"], "in_rps": round(p.get("inputRowsPerSecond") or 0, 1),
                   "proc_rps": round(p.get("processedRowsPerSecond") or 0, 1),
                   "batch_ms": p["durationMs"].get("triggerExecution")}
            lines.append(rec)
            with log.open("a") as f:
                f.write(json.dumps(rec) + "\n")
        if lines:
            print("  " + " | ".join(f"{r['query']}: batch {r['batch']} rows {r['rows']} in {r['in_rps']}/s proc {r['proc_rps']}/s ({r['batch_ms']} ms)" for r in lines), flush=True)


def main(layers=("bronze", "silver", "gold"), monitor_every=15):
    spark = get_spark()
    queries = []
    if "bronze" in layers:
        queries.append(bronze.start(spark))
    if "silver" in layers:
        _wait_for(spark, TABLES["bronze"])
        queries += silver.start(spark)
    if "gold" in layers:
        _wait_for(spark, TABLES["silver"])
        queries += gold.start(spark)
    print(f"running: {[q.name for q in queries]}  ·  Spark UI: http://localhost:4040  ·  Ctrl+C to stop (safe: checkpoints resume)")
    threading.Thread(target=_monitor, args=(spark, monitor_every), daemon=True).start()
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("stopping queries...")
        for q in spark.streams.active:
            q.stop()
```

- Starts the queries in one Spark application: bronze, then silver and dead-letter (after the bronze table exists), then the two gold queries.
- `_monitor` prints and logs each query's batch, rows, input rate and processing rate every 15 s to `lake/_metrics/progress.jsonl` (used by the report and the load test).
- `awaitAnyTermination()`: if **any** query fails, the app stops. That is **fail-fast** (good for correctness). In production you would run layers as separate jobs so a gold bug cannot stop ingestion (lesson from F9).

Run it in **terminal 1**:

```bash
python -m telemetry.cli stream
```

In **terminal 2** (activate the venv there too), send data:

```bash
python -m telemetry.cli produce --rate 500 --engines 20
```

Open the **Spark UI** at http://localhost:4040 → **Structured Streaming** tab: five queries, their input and processing rates, batch durations.

"Current batch is falling behind" warnings in the first minute are normal (JVM warm-up, F7).

**Check:** after a minute, `ls lake/silver/telemetry lake/gold/engine_current_state` shows Delta folders with `_delta_log`.

---

<a id="verify"></a>

# 15. Report and verify: prove it

Create `telemetry/report.py`:

```python
"""Read the Delta tables (with delta-rs, no Spark needed) and print a health report of the pipeline."""
import json

import pyarrow.compute as pc
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from .config import DATA_DIR, LAKE, TABLES


def table(name, columns=None):
    try:
        return DeltaTable(str(TABLES[name])).to_pyarrow_table(columns=columns)
    except (TableNotFoundError, Exception):
        return None


def _pct(arr, q):
    return round(pc.quantile(arr, q=q)[0].as_py(), 2) if arr is not None and len(arr) else None


def report() -> dict:
    out = {}
    b = table("bronze", ["offset"])
    out["bronze_rows"] = b.num_rows if b is not None else 0
    s = table("silver", ["event_id", "event_time", "kafka_ts", "ingest_ts", "silver_ts"])
    if s is not None and s.num_rows:
        out["silver_rows"] = s.num_rows
        out["silver_distinct_event_ids"] = len(pc.unique(s["event_id"]))
        out["silver_duplicates"] = out["silver_rows"] - out["silver_distinct_event_ids"]
        ms = lambda a, b_: pc.divide(pc.cast(pc.subtract(s[a], s[b_]), "int64"), 1000.0)  # micros -> ms
        e2e = ms("silver_ts", "event_time")
        out["latency_event_to_silver_ms"] = {"p50": _pct(e2e, 0.5), "p95": _pct(e2e, 0.95), "p99": _pct(e2e, 0.99)}
        k2b = ms("ingest_ts", "kafka_ts")
        out["latency_kafka_to_bronze_ms"] = {"p50": _pct(k2b, 0.5), "p95": _pct(k2b, 0.95)}
    d = table("dlq", ["dlq_reason"])
    out["dead_letter"] = {r["dlq_reason"]: r["count"] for r in d.group_by("dlq_reason").aggregate([("dlq_reason", "count")]).rename_columns(["dlq_reason", "count"]).to_pylist()} if d is not None and d.num_rows else {}
    st = table("engine_state", ["alert_level"])
    if st is not None:
        lv = st["alert_level"].to_pylist()
        out["engines"] = {"total": len(lv), "ok": lv.count(0), "warning": lv.count(1), "critical": lv.count(2)}
    a = table("alerts", ["alert_id"])
    if a is not None:
        out["alerts"] = {"rows": a.num_rows, "distinct": len(pc.unique(a["alert_id"]))}
    f = table("fleet_minute", ["events"])
    if f is not None:
        out["fleet_minute_windows"] = f.num_rows
    prog = LAKE / "_metrics" / "progress.jsonl"
    if prog.exists():
        recs = [json.loads(x) for x in prog.read_text().splitlines() if x.strip()]
        peak = {}
        for r in recs:
            if r["rows"] and r["proc_rps"] > peak.get(r["query"], 0):
                peak[r["query"]] = r["proc_rps"]
        out["peak_processed_rows_per_s"] = peak
    print(json.dumps(out, indent=2))
    return out


def verify(run_id: str | None = None) -> bool:
    """Exactly-once proof: every valid event the producer sent is in silver exactly once, and nothing else is."""
    mdir = DATA_DIR / "manifests"
    runs = sorted(mdir.glob("*.ids"))
    if not runs:
        print("no producer manifests found: run the producer without --no-manifest first")
        return False
    files = [mdir / f"{run_id}.ids"] if run_id else runs
    expected = set()
    for p in files:
        expected |= set(x for x in p.read_text().split("\n") if x)
    s = table("silver", ["event_id"])
    got = s["event_id"].to_pylist() if s is not None else []
    got_set = set(got)
    missing, extra, dupes = expected - got_set, got_set - expected, len(got) - len(got_set)
    ok = not missing and not dupes and not extra
    print(json.dumps({"manifests": [p.name for p in files], "expected_valid_events": len(expected), "silver_rows": len(got),
                      "missing": len(missing), "unexpected": len(extra), "duplicates": dupes, "result": "PASS" if ok else "FAIL"}, indent=2))
    if missing:
        print("  e.g. missing:", sorted(missing)[:3], "(if the stream is still catching up, wait and re-run)")
    return ok
```

### Tech box: delta-rs (`deltalake` package)
- **What:** a Rust implementation of Delta Lake with a Python API.
- **Why:** the report, verify and dashboard read tables **without starting Spark** (milliseconds instead of a 20-second JVM start), and without disturbing the running stream.
- **Alternatives:** Spark (slow to start), DuckDB's Delta extension (also good).

**`report`** prints counts per layer, duplicates in silver, dead letters by reason, latency percentiles (p50/p95/p99), engines by alert level, alerts (rows vs distinct), and peak rates.

**`verify`** is the exactly-once proof: the set of IDs in the producer's manifests vs the IDs in silver:
- **missing** = sent but not in silver (data loss)
- **unexpected** = in silver but never sent as valid (a bad record leaked, or a stale replay)
- **duplicates** = silver rows minus distinct IDs

```bash
python -m telemetry.cli report
python -m telemetry.cli verify
```

**Why percentiles, not averages?** One slow batch or the 3% of events deliberately delayed 30 s hide inside an average. p50/p95/p99 tell the real story (F12).

**Check:** `verify` prints `PASS` once the stream has caught up (if it says missing, wait a few seconds and run it again).

---

<a id="demo"></a>

# 16. The crash demo: exactly-once under chaos

Stop the stream from chapter 14 (Ctrl+C). Create `scripts/demo.sh`:

```bash
#!/usr/bin/env bash
# End-to-end demo with a hard crash in the middle. Proves exactly-once delivery into silver.
# Prereqs: `docker compose up -d` (Kafka running) and the Python venv activated.
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PYTHON:-python}
LOG=data/stream.log
mkdir -p data

start_stream() { nohup $PY -m telemetry.cli stream >> "$LOG" 2>&1 & echo $! > data/stream.pid; }
wait_table()   { until [ -d "lake/$1/_delta_log" ]; do sleep 2; done; }

echo "== 1. clean slate: delete lake, recreate topic"
$PY -m telemetry.cli reset
$PY -m telemetry.cli delete-topic || true
sleep 3
$PY -m telemetry.cli create-topic --partitions 6
: > "$LOG"

echo "== 2. start the streaming app (bronze -> silver + dead letter -> gold)"
start_stream
wait_table bronze/telemetry_raw

echo "== 3. wave 1: whole NASA fleet (200 engines) with chaos: 5% duplicates, 3% late by 30 s, 1% bad"
$PY -m telemetry.cli produce --rate 3000 --chaos --start-loop 0 &
PRODUCER=$!
sleep 12
echo "== 4. CRASH: kill -9 the streaming app while data is flowing"
kill -9 "$(cat data/stream.pid)" || true
sleep 5
echo "== 5. restart: checkpoints resume exactly where each query stopped"
start_stream
wait $PRODUCER

echo "== 6. wave 2: 40 new engines at 100 events/s for ~2.5 min, so the watermark advances and minute windows close"
$PY -m telemetry.cli produce --rate 100 --engines 20 --max-events 15000 --start-loop 1

echo "== 7. wait for the pipeline to drain, then verify exactly-once"
for i in $(seq 1 60); do $PY -m telemetry.cli verify > /dev/null 2>&1 && break; sleep 5; done
$PY -m telemetry.cli verify
$PY -m telemetry.cli report
echo "Streaming app still running (pid $(cat data/stream.pid)). Dashboard: streamlit run dashboard/app.py"
```

```bash
chmod +x scripts/demo.sh
./scripts/demo.sh            # about 6-8 minutes
```

**What happens:**
1. Clean slate: delete the lake, recreate the topic.
2. Start the streaming app in the background.
3. Wave 1: the whole fleet (200 engines) at 3,000 events/s **with chaos**.
4. **12 seconds in: `kill -9`** the streaming app. No graceful shutdown: like a machine losing power.
5. Restart it 5 seconds later. Checkpoints resume every query exactly where it stopped.
6. Wave 2: 40 new engines slowly, so the watermark advances and minute windows close.
7. Wait until `verify` passes, then print `verify` and `report`.

**What you should see** (these are the numbers from re-running this exact project):

| Measure | Value |
|---|---|
| Messages sent (wave 1) | 47,578 = 44,913 valid unique + 2,227 duplicates + 1,386 late (30 s) + 438 bad |
| Wave 2 | 9,874 messages |
| Expected valid unique events | **54,787** |
| In silver | **54,787: 0 missing, 0 duplicates, 0 unexpected → PASS** |
| Dead letters | 438 = 145 MALFORMED_JSON + 154 MISSING_FIELD + 139 OUT_OF_RANGE |
| Alerts | rows = distinct (each engine × level once). The number grows while gold catches up: 425 when `verify` first passed in the original run, 480 (= 240 engines × 2 levels) once every engine has reached CRITICAL |
| Fleet-minute windows | Often **0** right after the demo. Correct behaviour, see F11 in Troubleshooting for how to see them |

Message counts are produced by a fixed random seed, so they should match exactly. Timing-dependent numbers (alerts so far, latency) will differ. What must hold on every machine: `verify` says **PASS** with 0 missing, 0 duplicates, 0 unexpected, and the dead letters add up to the bad messages sent. That is the claim.

**Why this matters:** "exactly-once" is the most over-claimed phrase in data engineering. You **tested** it against duplicates, late data, bad data and a hard crash, and **verified** it against an independent manifest.

**Check:** the final `verify` says `PASS`.

---

<a id="dashboard"></a>

# 17. Dashboard, Docker image and CI

## The dashboard

### Tech box: Streamlit
- **What:** a Python library that turns a script into a web app.
- **Why here:** one file, pure Python, enough for a live fleet view. It reads the Delta tables with delta-rs, so it needs no Spark.
- **Alternatives:** Power BI / Databricks SQL dashboards (what Cummins would use in production on gold tables), Grafana (great for time-series metrics), Superset.
- **Impact:** a demo you can show in an interview in 10 seconds.

Create `dashboard/app.py`:

```python
"""Live fleet dashboard: reads the gold/silver Delta tables directly (delta-rs, no Spark needed).
Run:  streamlit run dashboard/app.py   ->  http://localhost:8501
"""
import sys
from pathlib import Path

import pandas as pd
import streamlit as st
from deltalake import DeltaTable

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from telemetry.config import TABLES  # noqa: E402

st.set_page_config(page_title="Turbofan Telemetry", layout="wide")


def load(name, columns=None):
    try:
        return DeltaTable(str(TABLES[name])).to_pandas(columns=columns)
    except Exception:
        return pd.DataFrame()


@st.fragment(run_every=10)
def live():
    state = load("engine_state")
    alerts = load("alerts")
    dlq = load("dlq", ["dlq_reason"])
    fleet = load("fleet_minute")
    c = st.columns(5)
    c[0].metric("Engines reporting", len(state))
    c[1].metric("Critical", int((state.alert_level == 2).sum()) if len(state) else 0)
    c[2].metric("Warning", int((state.alert_level == 1).sum()) if len(state) else 0)
    c[3].metric("Alerts raised", len(alerts))
    c[4].metric("Dead-lettered events", len(dlq))

    left, right = st.columns([3, 2])
    with left:
        st.subheader("Fleet health per minute (event time)")
        if len(fleet):
            f = fleet.sort_values("window_start").pivot_table(index="window_start", columns="fleet", values="avg_health_index")
            st.line_chart(f)
        else:
            st.caption("Windows appear once the watermark has passed them (2 minutes after newer events arrive).")
        st.subheader("Engines closest to failure")
        if len(state):
            top = state.sort_values("ewma_health", ascending=False).head(15)
            top["status"] = top.alert_level.map({0: "OK", 1: "WARNING", 2: "CRITICAL"})
            st.dataframe(top[["engine_id", "fleet", "last_cycle", "ewma_health", "status", "s11", "s4"]], hide_index=True, use_container_width=True)
    with right:
        st.subheader("Latest alerts")
        if len(alerts):
            st.dataframe(alerts.sort_values("event_time", ascending=False).head(20)[[c for c in ["engine_id", "level", "fault_mode", "cycle", "ewma_health", "event_time"] if c in alerts.columns]],
                         hide_index=True, use_container_width=True)
        st.subheader("Dead-letter reasons")
        if len(dlq):
            st.bar_chart(dlq.dlq_reason.value_counts())


st.title("Turbofan Telemetry: fleet health")
st.caption("NASA C-MAPSS engines streamed through Kafka → Spark Structured Streaming → Delta Lake. Refreshes every 10 s.")
live()
```

```bash
streamlit run dashboard/app.py         # http://localhost:8501
```

`@st.fragment(run_every=10)` refreshes only the live part every 10 seconds. You see engines reporting, critical and warning counts, alerts, dead letters by reason, the engines closest to failure, and fleet health per minute.

## Option B: run everything in Docker (no Java/Python on your machine)

Create `Dockerfile`:

```dockerfile
# Python 3.12 + Java 17 + Spark/Delta/Kafka client: everything the pipeline needs, nothing installed on your laptop.
FROM python:3.12-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
# JAVA_HOME works on both Intel/AMD (amd64) and Apple Silicon (arm64) images
RUN ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
```

- Python 3.12 + Java 17 + all requirements in one image. The `ln -s ... default-java` line makes `JAVA_HOME` work on both Intel and Apple Silicon.
- The `app` service in `docker-compose.yml` mounts your folder, keeps Spark's downloaded jars in a volume (`ivy-cache`) and connects to Kafka at `kafka:19092` (the inside-Docker address from chapter 3).

```bash
docker compose --profile app run --rm app download
docker compose --profile app run --rm app create-topic
docker compose --profile app run --rm app stream          # in one terminal
docker compose --profile app run --rm app produce --rate 500
```

## Continuous integration (CI)

### Tech box: GitHub Actions
- **What:** GitHub runs commands on every push and pull request.
- **Why here:** every change is tested automatically; the green check on your repo is visible proof to reviewers.
- **Alternatives:** Azure DevOps Pipelines (common at enterprises like Cummins), GitLab CI, Jenkins.
- **Impact:** broken code cannot quietly reach `main`.

Create `.github/workflows/ci.yml`:

```yaml
name: ci
on: [push, pull_request]
jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.12" }
      - run: pip install pytest==8.3.4
      - run: python -m pytest -q tests      # pure-Python tests: no Spark/Kafka needed
```

It installs only pytest and runs the unit tests: no Spark, no Kafka, finishes in seconds. The Spark parity test is skipped there (`importorskip`) and is run locally. A fuller CI would add a linter (ruff) and a small Spark integration test.

**Check:** the dashboard shows your engines, and after you push to GitHub (chapter 20) the Actions tab shows a green run.

---

<a id="loadtest"></a>

# 18. Load test and capacity plan

A pipeline that works for 50,000 events is not proof that it works for 5 million. Measure it.

Create `scripts/loadtest.py`:

```python
"""Load test (Milestone T2). Measures what this pipeline sustains on the current machine.

Phase 1, catch-up: put a large backlog in Kafka first, then start the full pipeline (bronze, silver, dead letter,
gold) and time how fast it drains through silver. This is the maximum sustained processing rate.
Phase 2, steady state: with the pipeline running, produce at fixed rates and measure end-to-end latency
(event_time set by the producer -> row committed in silver) and whether the pipeline keeps up.

Prereqs: Kafka running (docker compose up -d), nothing else streaming.
    python scripts/loadtest.py --loops 10 --rates 1000 3000
Writes docs/loadtest-results.json and prints a markdown table for docs/capacity-plan.md.
"""
import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from telemetry.config import LAKE, MAX_OFFSETS_PER_TRIGGER, PARTITIONS, TABLES, TRIGGER  # noqa: E402

PY = sys.executable


def cli(*args, **kw):
    return subprocess.run([PY, "-m", "telemetry.cli", *args], cwd=ROOT, check=True, capture_output=True, text=True, **kw)


def silver_count(engine_suffix=None):
    from deltalake import DeltaTable
    p = TABLES["silver"]
    if not (p / "_delta_log").exists():
        return 0
    dt = DeltaTable(str(p))
    if engine_suffix is None:
        return sum(a["num_records"] for a in dt.get_add_actions(flatten=True).to_pylist())
    t = dt.to_pyarrow_table(columns=["engine_id"])
    import pyarrow.compute as pc
    return pc.sum(pc.ends_with(t["engine_id"], engine_suffix).cast("int64")).as_py() or 0


def produced(out: str) -> int:
    for line in out.splitlines():
        if '"messages_sent"' in line:
            return int(line.split(":")[1].strip(" ,"))
    raise RuntimeError("producer summary not found:\n" + out[-2000:])


def latency(loop_ids):
    from deltalake import DeltaTable
    import pyarrow.compute as pc
    t = DeltaTable(str(TABLES["silver"])).to_pyarrow_table(columns=["engine_id", "event_time", "silver_ts"])
    mask = None
    for n in loop_ids:
        m = pc.ends_with(t["engine_id"], f"-L{n}")
        mask = m if mask is None else pc.or_(mask, m)
    t = t.filter(mask)
    ms = pc.divide(pc.subtract(t["silver_ts"].cast("int64"), t["event_time"].cast("int64")), 1000).to_pylist()
    ms.sort()
    q = lambda p: round(ms[min(len(ms) - 1, int(p * len(ms)))]) if ms else None  # noqa: E731
    return {"rows": len(ms), "p50_ms": q(0.5), "p95_ms": q(0.95), "p99_ms": q(0.99)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--loops", type=int, default=10, help="fleet replays in the backlog (~45k events each)")
    ap.add_argument("--rates", type=int, nargs="*", default=[1000, 3000])
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--layers", nargs="*", default=["bronze", "silver", "gold"], help="which layers to run")
    ap.add_argument("--out", default="docs/loadtest-results.json")
    a = ap.parse_args()

    shutil.rmtree(LAKE, ignore_errors=True)
    subprocess.run([PY, "-m", "telemetry.cli", "delete-topic"], cwd=ROOT, capture_output=True)
    time.sleep(3)
    cli("create-topic", "--partitions", str(PARTITIONS))

    # ---- phase 1: backlog
    t0 = time.time()
    out = cli("produce", "--rate", "0", "--loops", str(a.loops), "--start-loop", "100", "--no-manifest").stdout
    n = produced(out)
    prod_s = time.time() - t0
    print(f"backlog: {n:,} events produced in {prod_s:.1f}s ({n / prod_s:,.0f}/s)", flush=True)

    log = open(ROOT / "data" / "loadtest-stream.log", "w")
    stream = subprocess.Popen([PY, "-m", "telemetry.cli", "stream", *a.layers], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    started = time.time()
    samples = []
    try:
        while True:
            c = silver_count()
            samples.append((time.time(), c))
            if c >= n:
                break
            if time.time() - started > 1800:
                raise TimeoutError("backlog did not drain in 30 minutes")
            time.sleep(2)
        done = time.time()
        first = next(ts for ts, c in samples if c > 0)
        # Wall-clock throughput including JVM start-up: conservative, and not fooled by a few huge batches.
        # (Measuring between 10% and 90% of the backlog is misleading here: with up to 200k rows per batch,
        # that window can fall between just two commits.)
        wall = n / (done - started)
        print(f"drained {n:,} events: first silver rows after {first - started:.0f}s, all after {done - started:.0f}s "
              f"(cold start included): {wall:,.0f} events/s end to end", flush=True)

        # ---- phase 2: steady state at fixed rates
        steady = []
        for i, rate in enumerate(a.rates):
            loop0 = 200 + i * 10
            loops = max(1, -(-rate * a.seconds // 44000))
            t1 = time.time()
            out = cli("produce", "--rate", str(rate), "--loops", str(loops), "--start-loop", str(loop0),
                      "--max-events", str(rate * a.seconds), "--no-manifest").stdout
            sent = produced(out)
            send_s = time.time() - t1
            ids = list(range(loop0, loop0 + loops))
            t2 = time.time()
            while sum(silver_count(f"-L{x}") for x in ids) < sent and time.time() - t2 < 600:
                time.sleep(2)
            lag_s = time.time() - t2
            lat = latency(ids)
            steady.append({"target_rate": rate, "sent": sent, "actual_rate": round(sent / send_s), "drain_after_send_s": round(lag_s, 1), **lat})
            print(f"steady {rate}/s: sent {sent:,} ({sent / send_s:,.0f}/s), silver caught up {lag_s:.0f}s after the producer stopped, "
                  f"latency p50 {lat['p50_ms']} ms p95 {lat['p95_ms']} ms", flush=True)
    finally:
        stream.terminate()
        try:
            stream.wait(30)
        except subprocess.TimeoutExpired:
            stream.kill()

    peaks = {}
    prog = LAKE / "_metrics" / "progress.jsonl"
    batch_ms = {}
    if prog.exists():
        for line in prog.read_text().splitlines():
            r = json.loads(line)
            if r["rows"]:
                peaks[r["query"]] = max(peaks.get(r["query"], 0), r["proc_rps"])
                batch_ms.setdefault(r["query"], []).append(r["batch_ms"])
    res = {
        "machine": {"cpus": os.cpu_count(), "ram_gb": round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30),
                    "python": platform.python_version(), "os": platform.system()},
        "settings": {"partitions": PARTITIONS, "trigger": TRIGGER, "max_offsets_per_trigger": MAX_OFFSETS_PER_TRIGGER,
                     "shuffle_partitions": os.environ.get("TT_SHUFFLE_PARTITIONS", "default"), "layers": a.layers},
        "backlog": {"events": n, "producer_events_per_s": round(n / prod_s), "first_rows_after_s": round(first - started),
                    "drain_s": round(done - started), "wall_clock_events_per_s": round(wall)},
        "steady": steady,
        "peak_processed_rows_per_s": peaks,
        "median_batch_ms": {q: sorted(v)[len(v) // 2] for q, v in batch_ms.items()},
    }
    (ROOT / a.out).write_text(json.dumps(res, indent=2))
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
```

Stop any running stream first, then:

```bash
python scripts/loadtest.py --loops 10 --rates 1000 3000       # ~10-15 minutes
python scripts/loadtest.py --layers bronze silver --loops 5 --out docs/loadtest-results-no-gold.json
```

**Two tests:**
1. **Backlog drain:** put ~450,000 events in Kafka, then start the pipeline cold and time how long until all are in silver. This is the maximum processing rate.
2. **Steady state:** produce at 1,000 and 3,000 events/s for 60 s each and measure end-to-end latency (event time → row in silver) and whether it keeps up.

**Results from the original 4-core machine** (yours will differ; write down your own):

| Measure | Result |
|---|---|
| Producer, one Python process | ~28,000 events/s |
| Backlog of ~453,000 events, cold start included | all in silver after ~59 s |
| Steady 3,000 events/s, all layers | keeps up; latency p50 17.8 s, p95 24.6 s |
| Steady 3,000 events/s, **without gold** | latency p50 8.2 s, p95 10.8 s |
| Gold batch duration | ~30 s (the bottleneck) |

**How the bottleneck was found:** every streaming batch is a Delta commit with a timestamp, so the gap between commits is each layer's real batch rate:

```bash
python -c "from deltalake import DeltaTable as D; [print(h['timestamp'], h['operation']) for h in D('lake/gold/engine_current_state').history(10)]"
```

Gold committed every ~30 s while bronze and silver slipped from 5 s to ~6.6 s because they shared 4 cores with gold. Running the same test without gold brought them back to 5 s: a **controlled experiment** confirming the cause.

**The production decision:** run gold as its own job with its own cores and a 30-second trigger (engine health changes once per flight, so 30 s is plenty); keep bronze and silver at 5 s.

Write `docs/capacity-plan.md` with your numbers, then a sizing for 10,000 engines: 10,000 events/s (plan 20,000 at peak), ~900 bytes each → ~9 MB/s → ~780 GB/day in Kafka, ~130 GB/day in Delta (Parquet compresses ~6×). From your measured per-core rates, estimate cores per layer with headroom, and say you would re-test on the target cluster.

A measured metric can lie: an early version reported "145 trillion events/s" because a whole backlog landed in one micro-batch and the formula divided by almost zero (F16). Always sanity-check a number against a second source.

**Check:** you have your own numbers in `docs/capacity-plan.md` and can say what the bottleneck is and how you proved it.

---

<a id="trouble"></a>

# 19. Troubleshooting: every error you might hit

These happened when the project was first built, or when it was rebuilt to write this guide.

| # | Symptom | Cause | Fix |
|---|---|---|---|
| F1 | `failed to connect to the docker API ... docker.sock` | Docker installed but not running | Open Docker Desktop; `docker info` must work |
| F2 | `429 Too Many Requests` pulling images | Docker Hub limit on anonymous pulls | Wait, or sign in with a free Docker account |
| F3 | Download hangs or 404 | NASA moved the link | The code uses the PHM S3 mirror; without internet the synthetic fallback runs |
| F4 | PySpark install or import fails on Python 3.13 | Spark 3.5 does not support 3.13 | Use the pinned PySpark 4.0.1 + delta-spark 4.0.0 |
| F5 | `Broker handle destroyed`, then topic does not exist | Admin client garbage-collected too early | Keep the client in a variable until `result()` |
| F6 | `argument layers: invalid choice: "['bronze', ...]"` | argparse list default with `choices` | Default applied in code, not in argparse |
| F7 | `Current batch is falling behind` | First batches are slow (JVM warm-up) | Normal at start; only a problem if it continues |
| F8 | `repos.spark-packages.org ... 403` | One jar mirror blocked | Harmless; Maven Central serves the jars |
| F9 | `ModuleNotFoundError: No module named 'pyarrow'` in gold, then the whole app stops | Spark's Python workers used the system Python | `PYSPARK_PYTHON = sys.executable` in `spark.py`; restart (checkpoints resume) |
| F10 | Engines CRITICAL but **0 alerts**, no error | Lazy recompute re-read the state table after it was updated | `localCheckpoint(eager=True)`, write alerts first |
| F11 | `fleet_health_minute` has 0 rows | Append mode writes a window only after the watermark passes it; data stopped, so the watermark stopped | Keep a slow stream running for 3+ minutes (below) |
| F12 | p99 latency of minutes | It includes the 30 s late events and any downtime | Report p50/p95/p99, explain the tail |
| F13 | Duplicates after a second producer run | Old event IDs replayed after the 10-minute dedupe window | `--start-loop N` gives new IDs per run |
| F14 | 44% of FD003 engines never alert | Fan fault cancels the compressor pattern | Fault-mode aware index (v2) |
| F15 | `ValueError: sleep length must be non-negative` | Clock read twice at high rates | Read the clock once |
| F16 | Impossible throughput numbers | Rate computed over a window inside one micro-batch | Wall-clock throughput; cross-check with Delta history |
| new | `Failed to acquire idempotence PID ... Coordinator load in progress: retrying` | Kafka just started and is still loading | Harmless; the producer retries by itself |
| new | `ERROR Inbox: Ignoring error ... RejectedExecutionException` right after a crash or stop | Spark shutting down its executors | Harmless shutdown noise |
| — | `JAVA_HOME is not set` / `UnsupportedClassVersionError` | Java missing or too old | Install Java 17 or 21 |
| — | `NoBrokersAvailable` / connection refused on 9092 | Kafka not running, or wrong address | `docker compose ps`; use `localhost:9092` outside Docker, `kafka:19092` inside |
| — | Changed code in silver/gold and the stream fails to restart | Stateful logic changed, incompatible with the old checkpoint | `python -m telemetry.cli reset`, recreate the topic, re-run (in production: new checkpoint + backfill from bronze) |

**Seeing fleet windows after the demo (F11).** Keep the stream running and send a slow trickle for about 5 minutes, so event time moves past the windows:

```bash
python -m telemetry.cli produce --rate 15 --engines 10 --start-loop 2
python -m telemetry.cli report            # fleet_minute_windows is now > 0
```

**How many alerts should I see?** Each engine raises at most one WARNING and one CRITICAL. The count grows while gold catches up; once every engine has reached failure it is 2 × engines (480 for the 240 engines of the demo). The real check is `alerts.rows == alerts.distinct`.

---

<a id="docs"></a>

# 20. Document it and publish to GitHub

Good documentation is half of why this project impresses. Write these yourself, in your own words: writing them is how you make sure you understand the project.

| File | Contents |
|---|---|
| `README.md` | One-paragraph summary, the architecture sketch, **results table** (verify numbers, dead letters, alert quality, throughput), quick start commands, the command table, project layout, data source credit |
| `docs/architecture.md` | A diagram (Mermaid renders on GitHub), a "guarantee → mechanism" table (no loss: checkpoints; no partial files: Delta commits; duplicates: deterministic ID + watermark dedupe; bad data: dead-letter; ordering: key = engine_id; alerts once: MERGE on alert_id; bounded memory: watermarks + RocksDB; catch-up: maxOffsetsPerTrigger), and a table of tables with their grain |
| `docs/decisions.md` | Architecture Decision Records: one short paragraph per decision with the alternative and why it lost (Kafka, Spark + Delta, medallion, health index v2, dedupe window, dead-letter, foreachBatch + MERGE, event time, everything local, fault-mode labels) |
| `docs/build-log.md` | What you did, phase by phase, and **every failure you hit** with error, cause, fix and lesson |
| `docs/capacity-plan.md` | Your load-test numbers and sizing (chapter 18) |

**Credit the data:** NASA Prognostics Center of Excellence, *Turbofan Engine Degradation Simulation Data Set* (C-MAPSS), A. Saxena and K. Goebel (2008).

**Publish:**

```bash
git add .
git commit -m "Turbofan telemetry streaming pipeline"
```

Create an empty repository on GitHub (no README), then:

```bash
git remote add origin https://github.com/<your-username>/turbofan-telemetry.git
git branch -M main
git push -u origin main
```

Check that `data/`, `lake/` and `.venv/` are **not** in the repository (`.gitignore`), and that the Actions tab shows a green CI run.

**Optional: a project page.** The original repo also has `scripts/build_site.py` and a `site/` page (an interactive explorer of all 200 engines) published with GitHub Pages. Build it only after everything else works.

---

<a id="talk"></a>

# 21. Explain it in an interview

## The 60-second version

"I built a real-time predictive-maintenance pipeline on NASA's public turbofan dataset: 200 engines, each run until failure. A Python producer replays them as a live fleet into **Kafka**, keyed by engine so each engine stays in order. **Spark Structured Streaming** writes **Delta Lake** bronze, silver and gold tables: bronze keeps raw messages, silver validates, sends bad records to a dead-letter table with reason codes and deduplicates by a deterministic event ID, and gold keeps a smoothed health score per engine and raises each alert exactly once with MERGE. Alerts warn every engine a median of 44 cycles before failure. I tested it with duplicates, late and malformed messages and a `kill -9` in the middle, and verified against a producer manifest: 54,787 expected, 54,787 stored, zero lost, zero duplicated. A load test showed it sustains 3,000 events per second on 4 cores and that the stateful gold step is the bottleneck, which I'd split into its own job."

## The questions you will get, and where the answer is

| Question | Chapter |
|---|---|
| Why Kafka? Why key by engine? Why 6 partitions? | 3, 5, 8 |
| How exactly is exactly-once achieved, and how did you prove it? | 11, 12, 15, 16 |
| What happens if a duplicate arrives 20 minutes later? | 12 (watermark trade-off, F13) |
| Why bronze, silver and gold? | 11 |
| How does the health score work, and why EWMA? | 5, 7 |
| How did you find the FD003 problem and how do you know the fix is not overfitted? | 7 |
| Why foreachBatch + MERGE instead of built-in streaming state? | 13 |
| What was the hardest bug? | 13 (F10) |
| What is the bottleneck and how did you find it? | 18 |
| How would this run at Cummins? | below |

## How it would run at Cummins (Azure)

Devices or plant gateways → **IoT Hub / Event Hubs** (Kafka protocol, so the producer and Spark source barely change) → **Azure Databricks** jobs running the same bronze/silver/gold code → Delta tables on **ADLS Gen2** governed by **Unity Catalog** → **Power BI** or Databricks SQL dashboards, alerts to a maintenance system. Changes: managed Kafka with replication 3, Avro + schema registry instead of JSON, gold as a separate job with a 30 s trigger, OPTIMIZE for small files, monitoring of consumer lag and freshness, CI/CD with Databricks Asset Bundles.

## Be honest about what you did

Say what you built, what you measured and what you would change. If you rebuilt it from this guide, say so plainly and show you understand every decision. Strong next steps you can add yourself (and then talk about): normalise FD002/FD004 by operating condition; global dedupe with MERGE; a gold job split with its own trigger; Avro + schema registry; run it on Databricks Free Edition.

**Final check:** you can give the 60-second version without notes, and answer every question in the table.
