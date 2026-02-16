# Kafka Connect Data Types (Struct, Field, SourceRecord) - Quick Lesson

## What Are These Classes?

When you write a `DebeziumDeserializationSchema`, you encounter three unfamiliar imports:

```java
import org.apache.kafka.connect.data.Struct;       // the data container
import org.apache.kafka.connect.data.Field;         // a column descriptor
import org.apache.kafka.connect.source.SourceRecord; // the raw CDC event wrapper
```

These come from **Kafka Connect**, a framework for streaming data between Kafka and external systems. Debezium is built on top of Kafka Connect, so it uses Kafka Connect's data format internally — even when Kafka itself is not involved in your pipeline.

## Why Do We Need Them?

The Flink MySQL CDC connector uses Debezium under the hood:

```
MySQL binlog → Debezium (Kafka Connect framework) → SourceRecord → your deserializer → CdcEvent
```

Debezium doesn't produce JSON or Java POJOs — it produces **Kafka Connect `SourceRecord`** objects containing **`Struct`** data. Your deserializer receives these `SourceRecord` objects and must extract the fields using the Kafka Connect API.

You can't avoid these classes — they are the interface between Debezium and your code.

## Why No Gradle Dependency Needed?

You might notice there's no `org.apache.kafka:connect-api` in `build.gradle`. That's because it's a **transitive dependency** — it comes along automatically:

```
Your build.gradle declares:
  implementation 'org.apache.flink:flink-connector-mysql-cdc:3.2.1'

Which pulls in:
  └── org.apache.flink:flink-connector-debezium:3.2.1
      └── io.debezium:debezium-embedded:1.9.8.Final
          └── org.apache.kafka:connect-api:3.2.0         ← Struct, Field, Schema
          └── org.apache.kafka:connect-json:3.2.0
          └── org.apache.kafka:connect-runtime:3.2.0
              └── org.apache.kafka:connect-transforms:3.2.0
```

### What Is a Transitive Dependency?

When library A depends on library B, and B depends on library C, then adding A to your project automatically makes C available too. You don't need to declare C explicitly.

```
You declare:         flink-connector-mysql-cdc
                              │
Gradle resolves:              ▼
                     flink-connector-debezium
                              │
                              ▼
                     debezium-embedded
                              │
                              ▼
                     kafka connect-api  ← Struct, Field, SourceRecord live here
```

You can verify this by running:

```bash
./gradlew :flinkConnectorsApp:dependencyInsight --dependency connect-api
```

This shows the full chain of who pulled in `connect-api` and why it's on your classpath.

### Should You Declare It Explicitly?

Generally no, for two reasons:
1. **Version alignment** — the CDC connector was tested with a specific `connect-api` version. Declaring your own might introduce a version conflict.
2. **Simplicity** — fewer declared dependencies means less to maintain.

The only time you'd declare it explicitly is if you need a specific version that differs from what the CDC connector provides (rare and risky).

## The Three Classes Explained

### 1. `SourceRecord` — The Envelope

`SourceRecord` is the outermost wrapper for every CDC event. Think of it as an **envelope** containing the actual data plus metadata about where it came from.

```java
public void deserialize(SourceRecord record, Collector<CdcEvent> out) {
    // The envelope
    record.topic();           // Kafka topic name (e.g. "db_1.shipments")
    record.sourcePartition(); // source identity (which MySQL server)
    record.sourceOffset();    // binlog position (file + offset)

    // The actual data — always cast to Struct
    Struct value = (Struct) record.value();
}
```

**You almost never use `SourceRecord` directly** beyond casting `.value()` to `Struct`. The useful data is inside the `Struct`.

```
SourceRecord (envelope)
├── topic: "db_1.shipments"              ← routing metadata
├── sourcePartition: {server: "mysql1"}  ← which source
├── sourceOffset: {file: "bin.001", pos: 1234}  ← binlog position
│
└── value: Struct  ← the actual CDC event data (this is what you work with)
    ├── "op": "c"
    ├── "ts_ms": 1700000000000
    ├── "source": Struct { "db", "table" }
    ├── "before": Struct { ... } or null
    └── "after": Struct { ... } or null
```

### 2. `Struct` — The Data Container

`Struct` is Kafka Connect's **typed data container**. It's like a `Map<String, Object>` but with a schema attached that defines what fields exist and what types they have.

```java
Struct value = (Struct) record.value();

// ── Typed access (you specify the expected type) ──
String op     = value.getString("op");        // "c", "u", "d", "r"
Long tsMs     = value.getInt64("ts_ms");      // 1700000000000
Struct source = value.getStruct("source");    // nested Struct
Struct after  = value.getStruct("after");     // nested Struct (or null)

// ── Generic access (returns Object, you cast yourself) ──
Object val = value.get("op");                 // returns Object
```

#### Why Not Just Use a Map or JSON?

| | `Map<String, Object>` | JSON String | `Struct` |
|---|---|---|---|
| **Has schema?** | No — any key, any type | No — types inferred at parse time | Yes — schema defines exact fields and types |
| **Type safety** | None — everything is `Object` | None — everything is text | Typed accessors (`getString`, `getInt32`) |
| **Null handling** | `map.get("x")` returns null silently | Field might be absent or `"null"` string | Schema defines which fields are nullable |
| **Nested objects** | `Map<String, Map<...>>` messy casting | Parse nested JSON | `getStruct("field")` returns another `Struct` |
| **Schema evolution** | No support | No support | Schema tracks field additions/removals |

`Struct` exists because Kafka Connect needs a self-describing data format that can carry schema information alongside the data. When Debezium reads a MySQL row, it creates a `Struct` whose schema matches the table's column definitions.

#### Struct Access Methods

```java
Struct row = value.getStruct("after");  // the row data

// Each method returns a specific Java type
String name   = row.getString("origin");          // → "Shanghai"
Integer id    = row.getInt32("shipment_id");      // → 1
Long bigId    = row.getInt64("big_id");           // → 9999999999L
Short small   = row.getInt16("is_arrived");       // → 0 (MySQL TINYINT)
Double price  = row.getFloat64("price");          // → 29.99
Boolean flag  = row.getBoolean("active");         // → true
byte[] data   = row.getBytes("blob_col");         // → byte array

// Generic access — when you don't know the type ahead of time
Object unknown = row.get("some_column");          // → Integer, String, Short, etc.
```

**Common pitfall:** If you call the wrong typed accessor, you get a `DataException`:

```java
// Column "shipment_id" is an Integer in the Struct
row.getString("shipment_id");  // DataException! It's not a String.
row.getInt32("shipment_id");   // Correct → returns Integer
row.get("shipment_id");        // Also works → returns Object (Integer at runtime)
```

When you're unsure of the type (or need to handle any table generically), use `.get(field)` which returns `Object`, then check the type yourself.

#### Nested Structs

The CDC event is a Struct of Structs:

```java
Struct value = (Struct) record.value();       // top-level: op, ts_ms, source, before, after

Struct source = value.getStruct("source");    // nested: db, table, server_id, ...
String db    = source.getString("db");        // "db_1"
String table = source.getString("table");     // "shipments"

Struct after = value.getStruct("after");      // nested: all row columns
Integer id   = after.getInt32("shipment_id"); // 1
String origin = after.getString("origin");    // "Shanghai"
```

`before` and `after` are **nullable** — `before` is null for INSERTs (no previous row), `after` is null for DELETEs (no new row):

| Operation | `before` | `after` |
|-----------|----------|---------|
| INSERT (`c`) | null | row data |
| UPDATE (`u`) | old row | new row |
| DELETE (`d`) | old row | null |
| Snapshot (`r`) | null | row data |

### 3. `Field` — The Column Descriptor

`Field` describes a single column in the Struct's schema. It's used when you need to iterate over fields dynamically (the generic approach).

```java
Struct after = value.getStruct("after");

// Get the schema, then iterate its fields
for (Field field : after.schema().fields()) {
    field.name();        // column name: "shipment_id", "origin", ...
    field.index();       // position: 0, 1, 2, ...
    field.schema();      // the field's Schema (type info)
    field.schema().type(); // Schema.Type enum: INT32, STRING, INT16, ...

    // Get the value using the Field object
    Object val = after.get(field);  // typed value: Integer, String, Short, ...
}
```

#### When Do You Use `Field` Explicitly?

**Single-table deserializer (hardcoded) — you DON'T use Field:**

```java
// You already know the columns, access them directly by name
shipment.setShipmentId(struct.getInt32("shipment_id"));
shipment.setOrigin(struct.getString("origin"));
```

**Multi-table deserializer (generic) — you DO use Field:**

```java
// You don't know column names at compile time — iterate the schema
for (Field field : struct.schema().fields()) {
    String name = field.name();     // discovered at runtime
    Object val = struct.get(field); // generic access
    // ... convert to JSON based on Java type
}
```

#### Field vs Schema vs Struct Relationship

```
Schema (defines the structure)
├── name: "after"
├── type: STRUCT
└── fields: List<Field>
    ├── Field { name: "shipment_id", schema: Schema{type: INT32} }
    ├── Field { name: "origin",      schema: Schema{type: STRING} }
    ├── Field { name: "is_arrived",  schema: Schema{type: INT16} }  ← TINYINT(1)!
    └── ...

Struct (holds the actual data, linked to a Schema)
├── schema → Schema (above)
└── values: [1, "Shanghai", 0, ...]    ← indexed by Field.index()
```

Each `Field` has its own `Schema` describing its type. The `Schema.Type` enum includes:

| Schema.Type | Java Type | MySQL Column Types |
|-------------|-----------|-------------------|
| `INT8` | `Byte` | TINYINT |
| `INT16` | `Short` | SMALLINT, TINYINT(1) |
| `INT32` | `Integer` | INT, MEDIUMINT |
| `INT64` | `Long` | BIGINT, TIMESTAMP |
| `FLOAT32` | `Float` | FLOAT |
| `FLOAT64` | `Double` | DOUBLE, DECIMAL |
| `BOOLEAN` | `Boolean` | (rarely used by MySQL CDC) |
| `STRING` | `String` | VARCHAR, TEXT, CHAR |
| `BYTES` | `byte[]` | BLOB, BINARY |
| `STRUCT` | `Struct` | Nested objects (before, after, source) |

**Important gotcha:** MySQL `BOOLEAN` / `TINYINT(1)` maps to `INT16` (Short), NOT `BOOLEAN`. This is a Debezium behavior — see the [deserializer lesson](./debezium-deserializer-lesson.md#6-mysql-cdc-type-gotcha-tinyint1-is-not-boolean) for details.

## How It All Fits Together

```
MySQL CDC Source Connector
         │
         │ reads binlog, produces:
         ▼
    SourceRecord (envelope)
    ├── sourceOffset: binlog position
    └── value: Struct (top-level)
                 │
                 ├── getString("op") → "c"
                 ├── getInt64("ts_ms") → 1700000000000
                 ├── getStruct("source")
                 │     ├── getString("db") → "db_1"
                 │     └── getString("table") → "shipments"
                 ├── getStruct("before") → null (INSERT has no before)
                 └── getStruct("after")
                       │
                       ├── schema().fields() → [Field, Field, Field, ...]
                       │     iterate for generic access ────────┐
                       │                                        │
                       ├── getInt32("shipment_id") → 1          │  hardcoded
                       ├── getString("origin") → "Shanghai"     │  access
                       └── get("is_arrived") → Short(0)         │
                                                                │
                       for (Field f : schema().fields()) {      │  generic
                           f.name();  // column name            │  access
                           struct.get(f); // value              │
                       }  ◄─────────────────────────────────────┘
                             │
                             ▼
                    Your Deserializer converts to:
                    CdcEvent { op, table, before (JSON), after (JSON) }
```

## Why Kafka Connect Format (Not Just JSON)?

You might wonder: "Why doesn't Debezium just give me JSON directly?"

Debezium was originally designed as a **Kafka Connect connector** — it produces records into Kafka topics. Kafka Connect has its own internal data format (`SourceRecord` + `Struct`) that:

1. **Carries schema alongside data** — consumers know the exact types without guessing
2. **Supports schema evolution** — columns can be added/removed, and connectors can handle it
3. **Enables transforms** — Kafka Connect has a transform pipeline (`SMT`) that operates on Structs
4. **Is converter-agnostic** — the same Struct can be serialized to JSON, Avro, Protobuf, etc.

Flink's CDC connector reuses Debezium but **bypasses Kafka entirely** — it embeds Debezium in-process and feeds `SourceRecord` objects directly to your deserializer. The Kafka Connect data format is a side effect of this architecture, not something you chose.

```
Traditional Debezium:     MySQL → Debezium → Kafka Topic → Consumer
                                     (SourceRecord serialized to JSON/Avro)

Flink CDC Connector:      MySQL → Debezium (embedded) → SourceRecord → your deserializer
                                     (SourceRecord passed in-memory, no Kafka)
```

## Quick Reference

```java
// ── SourceRecord: the envelope ──
Struct value = (Struct) record.value();    // the only thing you usually need

// ── Struct: typed access by name ──
value.getString("op");                      // String field
value.getInt32("id");                       // Integer field
value.getInt64("ts_ms");                    // Long field
value.getInt16("small_num");                // Short field
value.getFloat64("price");                  // Double field
value.getBoolean("flag");                   // Boolean field (rare in MySQL CDC)
value.getStruct("source");                  // nested Struct
value.get("anything");                      // Object (generic)

// ── Field: for dynamic iteration ──
for (Field field : struct.schema().fields()) {
    String name = field.name();             // column name
    Schema.Type type = field.schema().type(); // INT32, STRING, etc.
    Object val = struct.get(field);         // the value
}
```

## TL;DR

| Question | Answer |
|----------|--------|
| **What is `SourceRecord`?** | Envelope wrapping a CDC event — cast `.value()` to `Struct` and move on |
| **What is `Struct`?** | Typed data container with a schema — like a Map but with known field types |
| **What is `Field`?** | Descriptor for a single column in a Struct's schema (name + type) |
| **Why not JSON?** | Debezium uses Kafka Connect's internal format; JSON is a serialization option, not the native format |
| **Why no Gradle dependency?** | Transitive: `flink-connector-mysql-cdc` → `debezium-embedded` → `kafka connect-api` |
| **When to use typed access?** | Single-table deserializer: `struct.getInt32("id")` |
| **When to use Field iteration?** | Multi-table generic deserializer: `struct.schema().fields()` loop |
| **TINYINT(1) gotcha** | Arrives as `Short` (`INT16`), not `Boolean` — handle with `struct.get()` |

---

## Takeaways

### What You Should Learn From This Doc

1. **Debezium's internal format is Kafka Connect Struct, not JSON** — JSON is just a serialization option. Inside your deserializer, you work with `Struct` objects that have attached schemas
2. **SourceRecord is the envelope, Struct is the data** — `sourceRecord.value()` gives you the top-level Struct containing `before`, `after`, `source`, and `op` fields
3. **Struct is schema-attached** — unlike a `Map`, every Struct knows its field names and types. You can iterate `struct.schema().fields()` to discover columns dynamically
4. **Transitive dependencies save you work** — `flink-connector-mysql-cdc` already pulls in `kafka-connect-api` through Debezium. Don't add it to Gradle manually or you risk version conflicts

### How This Helps You Understand the Flink Application

- You can now read `JsonCdcDeserializer.java` and understand what `sourceRecord.value()` returns — a Struct, not a String
- You understand why the deserializer casts to `Struct` and calls `.schema().fields()` — it's iterating the CDC event's columns dynamically
- You see why `struct.get(field)` returns `Object` — the generic accessor works for any column type, which is what the multi-table approach needs

### Other Benefits of This Knowledge

- **Write Kafka Connect transformations (SMTs)** — Single Message Transforms use the same Struct/Schema API to modify records in transit between connectors
- **Build custom Kafka Connect connectors** — source connectors produce `SourceRecord`, sink connectors consume them. This is the same API
- **Debug Debezium issues** — when CDC events look wrong, you can inspect the `Struct.schema()` to see what Debezium thinks the column types are
- **Understand Kafka Connect ecosystem** — Debezium is just one connector. Others (JDBC Sink, S3, Elasticsearch) all use the same Struct/SourceRecord format internally
