# Flink Sink2 API - From 0 to 100

## What Is a Sink?

A Sink is the **terminal operator** in a Flink pipeline — where data leaves Flink and goes somewhere else (database, file, message queue, API, etc.).

```
Source → Transform → Transform → Sink
                                  ↑
                          data exits Flink here
```

## The Sink2 API (Two Classes)

Flink's modern Sink API (`org.apache.flink.api.connector.sink2`) requires two classes:

```java
// 1. The Factory — creates writers, gets serialized and shipped to TaskManagers
public class MySink implements Sink<MyType> {
    public SinkWriter<MyType> createWriter(InitContext context) {
        return new MyWriter();
    }
}

// 2. The Worker — does the actual writing
class MyWriter implements SinkWriter<MyType> {
    public void write(MyType element, Context context) { ... }
    public void flush(boolean endOfInput) { ... }
    public void close() { ... }
}
```

**Why two classes?**

```
JobManager                              TaskManager 1          TaskManager 2
┌──────────────┐   serialize & ship    ┌──────────────┐      ┌──────────────┐
│ MySink       │ ────────────────────► │ MySink copy  │      │ MySink copy  │
│ (factory)    │                       │ .createWriter()     │ .createWriter()
│              │                       │      │        │      │      │        │
│ Serializable │                       │      ▼        │      │      ▼        │
└──────────────┘                       │ MyWriter #1   │      │ MyWriter #2   │
                                       │ (has DB conn) │      │ (has DB conn) │
                                       └──────────────┘      └──────────────┘
```

- `Sink` (factory) must be `Serializable` because Flink ships it over the network
- `SinkWriter` (worker) does NOT need to be serializable — it's created locally on each TaskManager
- Non-serializable resources (DB connections, HTTP clients) go in the writer, not the factory

## Minimal Example: Console Sink

The simplest possible sink — prints to stdout:

```java
public class ConsoleSink implements Sink<String> {
    @Override
    public SinkWriter<String> createWriter(InitContext context) {
        return new ConsoleWriter();
    }
}

class ConsoleWriter implements SinkWriter<String> {
    @Override
    public void write(String element, Context context) {
        System.out.println(element);   // called for each record
    }

    @Override
    public void flush(boolean endOfInput) {
        // nothing to flush — println is immediate
    }

    @Override
    public void close() {
        // nothing to clean up
    }
}
```

Usage:

```java
DataStream<String> stream = ...;
stream.sinkTo(new ConsoleSink());
```

## Real Example: Database Sink (Step by Step)

Let's build a Postgres sink from scratch, progressively adding concepts.

### Step 1: Naive — Write Every Record Immediately

```java
public class NaivePGSink implements Sink<CdcEvent> {
    private final String jdbcUrl;
    private final String user;
    private final String pass;

    public NaivePGSink(String jdbcUrl, String user, String pass) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.pass = pass;
    }

    @Override
    public SinkWriter<CdcEvent> createWriter(InitContext context) {
        return new NaivePGWriter(jdbcUrl, user, pass);
    }
}

class NaivePGWriter implements SinkWriter<CdcEvent> {
    private final HikariDataSource dataSource;

    NaivePGWriter(String jdbcUrl, String user, String pass) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(user);
        config.setPassword(pass);
        this.dataSource = new HikariDataSource(config);
    }

    @Override
    public void write(CdcEvent event, Context context) throws IOException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "INSERT INTO shipments (id, origin) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET origin = ?")) {
            ps.setInt(1, ...);
            ps.setString(2, ...);
            ps.setString(3, ...);
            ps.executeUpdate();   // executes immediately, auto-commits
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void flush(boolean endOfInput) {
        // nothing — each write() already committed
    }

    @Override
    public void close() throws Exception {
        dataSource.close();
    }
}
```

**Problem:** Every record triggers a separate DB round-trip + commit. At 1000 events/sec, that's 1000 commits/sec — the commit (fsync to disk) is the bottleneck.

### Step 2: Buffered — Write in Batches on Flush

```java
class BufferedPGWriter implements SinkWriter<CdcEvent> {
    private final HikariDataSource dataSource;
    private final List<CdcEvent> buffer = new ArrayList<>();   // accumulate here

    // constructor same as above...

    @Override
    public void write(CdcEvent event, Context context) {
        buffer.add(event);    // just buffer, no DB call
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        if (buffer.isEmpty()) return;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);                    // BEGIN transaction
            try {
                for (CdcEvent event : buffer) {
                    executeSql(conn, event);               // N statements, 1 transaction
                }
                conn.commit();                             // 1 commit for all N events
                buffer.clear();
            } catch (Exception e) {
                conn.rollback();                           // all-or-nothing
                throw e;
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws Exception {
        flush(true);            // flush remaining before shutdown
        dataSource.close();
    }
}
```

**Improvement:** N events share 1 commit. For 1000 events per checkpoint, that's 1000x fewer fsyncs.

### Step 3: The Lifecycle — When Does Flink Call Each Method?

```
Flink runtime calls your SinkWriter methods in this order:
─────────────────────────────────────────────────────────

    ┌─── createWriter() ──── writer instance created, DB pool opened
    │
    │    event 1 ──► write()  →  buffer: [e1]
    │    event 2 ──► write()  →  buffer: [e1, e2]
    │    event 3 ──► write()  →  buffer: [e1, e2, e3]
    │
    │    ─── checkpoint barrier arrives ───
    │
    │    Flink calls ► flush(endOfInput=false)
    │                    conn.setAutoCommit(false)
    │                    executeSql(e1)
    │                    executeSql(e2)
    │                    executeSql(e3)
    │                    conn.commit()
    │                    buffer.clear()
    │
    │    ─── checkpoint N completes (binlog offset persisted) ───
    │
    │    event 4 ──► write()  →  buffer: [e4]
    │    event 5 ──► write()  →  buffer: [e4, e5]
    │
    │    ─── checkpoint barrier arrives ───
    │
    │    Flink calls ► flush(endOfInput=false)
    │                    commit [e4, e5]
    │
    │    ─── job stopping ───
    │
    │    Flink calls ► close()
    │                    └── flush(endOfInput=true)  ← flush remaining
    │                    └── dataSource.close()      ← release resources
    └──────────────────────────────────────────────────
```

**Key rules:**
- `write()` is called once per incoming record — do lightweight work here (buffer, validate)
- `flush(false)` is called at every **checkpoint barrier** — do the heavy work here (DB writes)
- `flush(true)` is called once when the stream ends — same as flush but signals "no more data"
- `close()` is called when the writer is being shut down — release resources

## sinkTo After map vs sinkTo After keyBy

This is the core question. The difference is about **how events are distributed across parallel writer instances**.

### Option A: `stream.map(...).sinkTo(sink)`

```java
cdcStream
    .map(event -> transform(event))
    .sinkTo(new PGSink(...));
```

**What happens:**

```
Source (parallelism=1)           Sink (parallelism=4)
                              ┌─────────────────────────┐
                              │ Writer #1                │
                              │ buffer: [shipments e1,   │
                              │          orders e3]      │  ← mixed tables!
                         ┌──► │                          │
                         │    └─────────────────────────┘
                         │    ┌─────────────────────────┐
                         │    │ Writer #2                │
CDC events ─────────────►├──► │ buffer: [shipments e2,   │  ← mixed tables!
(round-robin             │    │          shipments e5]   │
 distribution)           │    └─────────────────────────┘
                         │    ┌─────────────────────────┐
                         │    │ Writer #3                │
                         └──► │ buffer: [orders e4,      │  ← mixed tables!
                              │          shipments e6]   │
                              └─────────────────────────┘
```

Without `keyBy`, Flink distributes records by **round-robin** (or rebalance). Events from the same table can end up in different writer instances.

**Implications:**
- Events for `shipments` row `id=1` might go to Writer #1 then Writer #3
- Two writers might try to upsert the same row concurrently → **race condition**
- An INSERT then DELETE for the same row might arrive at different writers → DELETE executes before INSERT → **wrong final state**

### Option B: `stream.keyBy(key).sinkTo(sink)`

```java
cdcStream
    .keyBy(CdcEvent::getTable)     // partition by table name
    .sinkTo(new PGSink(...));
```

**What happens:**

```
Source (parallelism=1)           Sink (parallelism=4)
                              ┌─────────────────────────┐
                              │ Writer #1                │
                              │ buffer: [shipments e1,   │
                              │          shipments e2,   │  ← only shipments!
                         ┌──► │          shipments e5]   │
                         │    └─────────────────────────┘
                         │    ┌─────────────────────────┐
CDC events ──────────────┤    │ Writer #2                │
(hash-partitioned        │    │ buffer: [orders e3,      │  ← only orders!
 by table name)          └──► │          orders e4,      │
                              │          orders e6]      │
                              └─────────────────────────┘
                              ┌─────────────────────────┐
                              │ Writer #3                │
                              │ buffer: []               │  ← idle (no key hashed here)
                              └─────────────────────────┘
```

`keyBy` uses a **hash function** on the key to deterministically route records. All events with the same key always go to the same writer instance.

**Implications:**
- All `shipments` events → same writer → **ordering preserved**
- All `orders` events → same writer → **ordering preserved**
- No concurrent writes to the same row → **no race conditions**
- Some writers may be idle if keys don't distribute evenly

### Comparison Table

| Aspect | `map().sinkTo()` (no keyBy) | `keyBy(table).sinkTo()` |
|--------|----------------------------|------------------------|
| **Distribution** | Round-robin (random) | Hash-partitioned by key |
| **Same-row ordering** | NOT guaranteed | Guaranteed |
| **Concurrent writes** | Possible — race conditions | Impossible for same key |
| **Parallelism utilization** | Even distribution | Uneven (depends on key cardinality) |
| **Use case** | Independent records (logs, metrics) | Records that must be ordered (CDC, transactions) |

### When Does It Matter?

**It matters for CDC / database mirroring:**

```
MySQL events arrive in order:
  1. INSERT shipments (id=1, origin="Shanghai")
  2. UPDATE shipments (id=1, origin="Tokyo")
  3. DELETE shipments (id=1)
```

With `keyBy(table)` → all three go to the same writer → executed in order → correct final state (row deleted).

Without `keyBy` → event 3 (DELETE) might execute before event 2 (UPDATE) in a different writer → final state has a phantom row with `origin="Tokyo"` → **inconsistent**.

**It doesn't matter for independent records:**

```java
// Each log line is independent — order doesn't matter
logStream.map(log -> format(log)).sinkTo(new ElasticsearchSink(...));
```

### Can You keyBy a More Granular Key?

Yes. `keyBy(table)` ensures per-table ordering. For even better parallelism, you could key by primary key:

```java
// Key by table + primary key → each row goes to one writer
cdcStream
    .keyBy(event -> event.getTable() + ":" + extractPK(event))
    .sinkTo(new PGSink(...));
```

This gives finer-grained partitioning — different rows of the same table can go to different writers, while events for the same row always go together:

```
keyBy(table)                          keyBy(table + PK)
─────────────────                     ─────────────────
Writer #1: ALL shipments events       Writer #1: shipments id=1, id=2
Writer #2: ALL orders events          Writer #2: shipments id=3, id=4
                                      Writer #3: orders id=1, id=2
                                      Writer #4: orders id=3
```

**Trade-off:** More parallelism, but each writer might handle multiple tables → more complex SQL generation. Our `PGSinker` already handles this (it reads the table name from each `CdcEvent`).

## sinkTo vs addSink (Legacy API)

You might see older code using `addSink()`:

```java
// OLD API (Flink < 1.12) — deprecated
stream.addSink(new SinkFunction<MyType>() {
    @Override
    public void invoke(MyType value, Context context) {
        // write to DB here — no flush concept!
    }
});

// NEW API (Sink2) — use this
stream.sinkTo(new MySink());
```

| | `addSink` (SinkFunction) | `sinkTo` (Sink2 API) |
|---|---|---|
| **Buffering/batching** | You implement yourself | Built-in via `write()` + `flush()` |
| **Checkpoint integration** | Manual (override `snapshotState()`) | Automatic (`flush()` called at barriers) |
| **Writer lifecycle** | Single class (SinkFunction) | Two classes (Sink + SinkWriter) |
| **Status** | Deprecated | Current standard |

## Complete Sink Implementation Checklist

When building a production sink:

```
1. Sink class (the factory)
   ├── implements Sink<T>
   ├── implements Serializable
   ├── stores config (JDBC URL, credentials, etc.)
   ├── fields must be Serializable (use intersection cast for Map.of() etc.)
   └── createWriter() → returns new SinkWriter

2. SinkWriter class (the worker)
   ├── implements SinkWriter<T>
   ├── constructor: open resources (connection pool, HTTP client)
   │
   ├── write(): buffer incoming records
   │   ├── validate early (fail-fast on bad data)
   │   └── do NOT make external calls here
   │
   ├── flush(): execute buffered writes
   │   ├── open transaction (setAutoCommit(false))
   │   ├── execute all buffered records IN ORDER
   │   ├── commit on success / rollback on failure
   │   └── clear buffer after commit
   │
   └── close(): cleanup
       ├── flush remaining (flush(true))
       └── close resources (connection pool, etc.)
```

## The Big Picture: Where Sink Fits in a Pipeline

```java
// 1. Environment
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(3000);

// 2. Source
DataStream<CdcEvent> stream = env.fromSource(mysqlSource, ...);

// 3. Transform (optional)
DataStream<CdcEvent> filtered = stream.filter(e -> e.getTable() != null);

// 4. Partition (important for ordering guarantees)
//
//    Option A: no keyBy — round-robin, no ordering guarantee
//    filtered.sinkTo(new PGSink(...));
//
//    Option B: keyBy — hash-partitioned, ordering guaranteed per key
//    filtered.keyBy(CdcEvent::getTable).sinkTo(new PGSink(...));

// 5. Execute
env.execute("My Job");
```

**Decision flowchart:**

```
Do records need to be processed in order?
│
├── No (independent records: logs, metrics, alerts)
│   └── stream.sinkTo(sink)                          ← no keyBy, max parallelism
│
└── Yes (stateful records: CDC, transactions, sessions)
    │
    ├── Order matters per table?
    │   └── stream.keyBy(event::getTable).sinkTo(sink)
    │
    └── Order matters per row?
        └── stream.keyBy(event -> table + ":" + pk).sinkTo(sink)
```

## TL;DR

| Question | Answer |
|----------|--------|
| **What is a Sink?** | Terminal operator — data exits Flink |
| **How many classes?** | 2: `Sink` (factory, serializable) + `SinkWriter` (worker, has resources) |
| **write()** | Called per record — buffer only, no external calls |
| **flush()** | Called at checkpoint — execute all buffered writes in one transaction |
| **close()** | Called at shutdown — flush remaining + release resources |
| **sinkTo after map** | Round-robin distribution — no ordering guarantee |
| **sinkTo after keyBy** | Hash-partitioned — ordering guaranteed per key |
| **Which to use?** | CDC/database mirroring → always `keyBy` first for correctness |
| **addSink vs sinkTo** | `addSink` is deprecated; use `sinkTo` with Sink2 API |

---

## Takeaways

### What You Should Learn From This Doc

1. **Sink2 API is a two-class pattern** — `Sink` (serializable factory) creates `SinkWriter` (stateful worker with resources). This separation exists because Flink serializes operators to ship them across the cluster
2. **write() is just buffering, flush() does the real work** — never make external calls in `write()`. Batch everything and execute in `flush()` within a single transaction for atomicity
3. **keyBy before sinkTo is critical for CDC** — without it, events for the same row can go to different subtasks and execute out of order, causing data corruption
4. **Checkpoint-aligned flushing** — Flink calls `flush()` at checkpoint boundaries, meaning your sink latency is bounded by your checkpoint interval (e.g., 3 seconds)

### How This Helps You Understand the Flink Application

- You can now read `PGSinker.java` / `SFSinker.java` and understand the `write()` → `flush()` → `close()` lifecycle without guessing
- You understand why `Main.java` uses `keyBy(CdcEvent::getTable)` before `.sinkTo()` — it's for ordering correctness, not performance
- You see why `PGSinker` implements `Sink<CdcEvent>` (factory) and `PostgresWriter` implements `SinkWriter<CdcEvent>` (worker) as separate classes

### Other Benefits of This Knowledge

- **Build sinks for any destination** — Elasticsearch, S3, Redis, HTTP APIs. The same Sink2 pattern applies: buffer in write, batch-execute in flush
- **Tune throughput vs latency** — increase checkpoint interval for bigger batches (higher throughput) or decrease for fresher data (lower latency)
- **Implement exactly-once sinks** — by combining checkpoint-aligned flushing with two-phase commit (Flink's `TwoPhaseCommittingSink` interface extends this pattern)
- **Migrate legacy sinks** — if you encounter `addSink` + `SinkFunction` in older codebases, you now know how to rewrite them using the modern Sink2 API
