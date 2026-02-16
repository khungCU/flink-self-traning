# Implementing DebeziumDeserializationSchema - Quick Lesson

## What is it?

`DebeziumDeserializationSchema<T>` is the interface you implement to tell Flink's MySQL CDC source **how to convert raw Debezium records into your Java objects**. Without it, Flink receives opaque `SourceRecord` objects and has no idea what to do with them.

```
MySQL binlog → Debezium → SourceRecord → ???  ← you fill this gap  → your Java type T
```

## Why is it needed?

The MySQL CDC source connector is built on top of **Debezium**, which internally uses the **Kafka Connect** data format. A Debezium CDC record is NOT JSON and NOT a POJO — it's a Kafka Connect `SourceRecord` containing nested `Struct` objects. You need a deserializer to extract the fields you care about.

## The Interface

```java
public interface DebeziumDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    // Called once per CDC event — extract fields and emit via collector
    void deserialize(SourceRecord record, Collector<T> out) throws Exception;

    // Tells Flink the output type (needed for serialization and optimization)
    TypeInformation<T> getProducedType();
}
```

Only two methods to implement. Let's understand each.

## The Debezium Record Structure

Before writing a deserializer, you need to understand what's inside a `SourceRecord`. The `record.value()` is a `Struct` with this shape:

```
SourceRecord.value() → Struct
├── "op"      : String           ← "c" (create), "u" (update), "d" (delete), "r" (snapshot read)
├── "ts_ms"   : Long             ← timestamp in milliseconds
├── "source"  : Struct           ← metadata
│   ├── "db"    : String         ← database name (e.g. "db_1")
│   └── "table" : String        ← table name (e.g. "shipments")
├── "before"  : Struct or null   ← row BEFORE the change (null for inserts)
│   ├── "shipment_id" : Integer
│   ├── "order_id"    : Integer
│   ├── "origin"      : String
│   └── "is_arrived"  : Short    ← MySQL TINYINT(1), NOT boolean!
└── "after"   : Struct or null   ← row AFTER the change (null for deletes)
    ├── "shipment_id" : Integer
    ├── "order_id"    : Integer
    ├── "origin"      : String
    └── "is_arrived"  : Short
```

### What is a `Struct`?

`Struct` is Kafka Connect's typed data container (from `org.apache.kafka.connect.data`). Think of it as a **typed Map with a schema attached**. Unlike a JSON object where everything is loosely typed, a `Struct` knows the exact type of every field.

```java
Struct value = (Struct) record.value();

// Access fields by name — returns typed Java objects
String op       = value.getString("op");          // typed access (String)
Long tsMs       = value.getInt64("ts_ms");         // typed access (Long)
Struct source   = value.getStruct("source");       // nested Struct
Struct before   = value.getStruct("before");       // nullable — null for inserts
Struct after    = value.getStruct("after");        // nullable — null for deletes

// From nested Struct
String table    = source.getString("table");
Integer id      = after.getInt32("shipment_id");
```

Key `Struct` access methods:

| Method | Returns | Use for |
|--------|---------|---------|
| `getString("field")` | `String` | VARCHAR, TEXT columns |
| `getInt32("field")` | `Integer` | INT columns |
| `getInt64("field")` | `Long` | BIGINT, timestamps |
| `getInt16("field")` | `Short` | SMALLINT, TINYINT |
| `getFloat64("field")` | `Double` | DOUBLE, DECIMAL |
| `getBoolean("field")` | `Boolean` | Only if truly BOOLEAN |
| `getStruct("field")` | `Struct` | Nested objects (before, after, source) |
| `get("field")` | `Object` | Generic — when you don't know the type |
| `schema()` | `Schema` | Access field names and types programmatically |

## Approach 1: Single-Table (Typed POJO)

The simplest approach — hardcode field-to-POJO mapping for one specific table.

**File:** `deserializer/ShipmentDebeziumDeserializer.java`

```java
public class ShipmentDebeziumDeserializer
        implements DebeziumDeserializationSchema<ShipmentCdcEvent> {

    private static final long serialVersionUID = 1L;

    @Override
    public void deserialize(SourceRecord record, Collector<ShipmentCdcEvent> out) throws Exception {
        Struct value = (Struct) record.value();
        if (value == null) return;

        ShipmentCdcEvent event = new ShipmentCdcEvent();
        event.setOp(value.getString("op"));

        Long tsMs = value.getInt64("ts_ms");
        if (tsMs != null) event.setTsMs(tsMs);

        Struct source = value.getStruct("source");
        if (source != null) {
            event.setDatabase(source.getString("db"));
            event.setTable(source.getString("table"));
        }

        Struct before = value.getStruct("before");
        if (before != null) event.setBefore(structToShipment(before));

        Struct after = value.getStruct("after");
        if (after != null) event.setAfter(structToShipment(after));

        out.collect(event);
    }

    // Hardcoded mapping: Struct fields → Shipment POJO
    private Shipment structToShipment(Struct struct) {
        Shipment s = new Shipment();
        s.setShipmentId(struct.getInt32("shipment_id"));
        s.setOrderId(struct.getInt32("order_id"));
        s.setOrigin(struct.getString("origin"));
        s.setDestination(struct.getString("destination"));

        // Gotcha: MySQL TINYINT(1) arrives as Short, not Boolean
        Object isArrived = struct.get("is_arrived");
        if (isArrived instanceof Boolean b)  s.setIsArrived(b);
        else if (isArrived instanceof Short v)   s.setIsArrived(v != 0);
        else if (isArrived instanceof Integer v)  s.setIsArrived(v != 0);

        return s;
    }

    @Override
    public TypeInformation<ShipmentCdcEvent> getProducedType() {
        return TypeInformation.of(ShipmentCdcEvent.class);
    }
}
```

**Pros:** Type-safe, compile-time checks, easy to debug.
**Cons:** One deserializer per table. 50 tables = 50 deserializers + 50 model classes.

## Approach 2: Multi-Table (Generic JSON)

Instead of mapping to a POJO, convert the Struct to a JSON string. Now one deserializer handles any table.

**File:** `deserializer/JsonCdcDeserializer.java`

```java
public class JsonCdcDeserializer implements DebeziumDeserializationSchema<CdcEvent> {

    private static final long serialVersionUID = 1L;
    private transient ObjectMapper objectMapper;

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) objectMapper = new ObjectMapper();
        return objectMapper;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<CdcEvent> out) throws Exception {
        Struct value = (Struct) record.value();
        if (value == null) return;

        CdcEvent event = new CdcEvent();
        event.setOp(value.getString("op"));

        Long tsMs = value.getInt64("ts_ms");
        if (tsMs != null) event.setTsMs(tsMs);

        Struct source = value.getStruct("source");
        if (source != null) {
            event.setDatabase(source.getString("db"));
            event.setTable(source.getString("table"));
        }

        // JSON strings instead of typed POJOs — works for any table
        Struct before = value.getStruct("before");
        if (before != null) event.setBefore(structToJson(before));

        Struct after = value.getStruct("after");
        if (after != null) event.setAfter(structToJson(after));

        out.collect(event);
    }

    // Schema-driven: iterates over whatever fields the Struct has
    private String structToJson(Struct struct) throws Exception {
        ObjectNode node = getObjectMapper().createObjectNode();
        for (Field field : struct.schema().fields()) {
            String name = field.name();
            Object val = struct.get(field);
            switch (val) {
                case null        -> node.putNull(name);
                case Integer i   -> node.put(name, i);
                case Long l      -> node.put(name, l);
                case Double d    -> node.put(name, d);
                case Float f     -> node.put(name, f);
                case Boolean b   -> node.put(name, b);
                case Short s     -> node.put(name, s);
                case byte[] bytes -> node.put(name, bytes);
                default          -> node.put(name, val.toString());
            }
        }
        return getObjectMapper().writeValueAsString(node);
    }

    @Override
    public TypeInformation<CdcEvent> getProducedType() {
        return TypeInformation.of(CdcEvent.class);
    }
}
```

**Pros:** One class handles all tables. Add new tables with zero code changes.
**Cons:** Loses compile-time type safety. JSON parsing needed downstream.

## Approach 3: Hybrid (Generic Source + Typed Downstream)

Best of both worlds — use the generic JSON deserializer at the source, then split into typed POJO streams downstream.

```
MySqlSource → JsonCdcDeserializer → CdcEvent (generic)
                                        │
                            ┌───────────┼───────────┐
                            ▼           ▼           ▼
                     filter("orders") filter("shipments") ...
                            │           │
                            ▼           ▼
                     map → Order   map → Shipment    (type-safe POJOs)
```

**Why not type-safe at the deserializer level?** `MySqlSource<T>` takes a **single** type parameter `T`. You can't produce `Order` for one record and `Shipment` for another from the same source — Java generics don't support union types like that. So type safety must happen **downstream**, not at ingestion.

### Option A: Filter + Map (simple, good for 2-3 tables)

```java
DataStream<CdcEvent> cdcStream = env
    .fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL Source");

// Split into typed streams
DataStream<Order> orders = cdcStream
    .filter(e -> "orders".equals(e.getTable()))
    .map(e -> objectMapper.readValue(e.getAfter(), Order.class));

DataStream<Shipment> shipments = cdcStream
    .filter(e -> "shipments".equals(e.getTable()))
    .map(e -> objectMapper.readValue(e.getAfter(), Shipment.class));

// Now you have full type safety downstream
orders.keyBy(Order::getCustomerId).process(...);
shipments.keyBy(Shipment::getOrderId).process(...);
```

Each `filter()` scans the full stream, so N tables = N passes. Fine for a few tables, inefficient for many.

### Option B: Side Outputs (single pass, good for 4+ tables)

```java
OutputTag<Order> orderTag = new OutputTag<>("orders") {};
OutputTag<Shipment> shipmentTag = new OutputTag<>("shipments") {};

SingleOutputStreamOperator<CdcEvent> main = cdcStream
    .process(new ProcessFunction<CdcEvent, CdcEvent>() {
        private transient ObjectMapper mapper;

        @Override
        public void processElement(CdcEvent event, Context ctx, Collector<CdcEvent> out)
                throws Exception {
            if (mapper == null) mapper = new ObjectMapper();
            String json = event.getAfter();
            if (json == null) return;

            switch (event.getTable()) {
                case "orders"    -> ctx.output(orderTag, mapper.readValue(json, Order.class));
                case "shipments" -> ctx.output(shipmentTag, mapper.readValue(json, Shipment.class));
                default          -> out.collect(event); // unmatched tables pass through
            }
        }
    });

DataStream<Order> orders = main.getSideOutput(orderTag);
DataStream<Shipment> shipments = main.getSideOutput(shipmentTag);
```

Side outputs route records in a **single pass** — one iteration over the stream instead of N filter operations. The main `Collector<CdcEvent>` handles unmatched tables (e.g. for generic mirroring), while `ctx.output()` sends matched tables to typed side streams.

**Pros:** Type-safe downstream. One deserializer for all tables. Single pass with side outputs.
**Cons:** More boilerplate than Approach 2. Still requires a POJO per table that needs typed processing.

## Side-by-Side Comparison

| Aspect | Single-Table (POJO) | Multi-Table (JSON) | Hybrid (Generic + Typed) |
|--------|---------------------|--------------------|--------------------------|
| **Output type** | `ShipmentCdcEvent` (typed before/after) | `CdcEvent` (JSON string before/after) | `CdcEvent` at source → typed POJOs downstream |
| **Struct → output** | Hardcoded field-by-field mapping | Schema-driven iteration (`struct.schema().fields()`) | Schema-driven at source, Jackson POJO binding downstream |
| **Adding a new table** | New deserializer + new model class | No code change | No deserializer change, add POJO + filter/side output |
| **Type safety** | Compile-time | Runtime (parse JSON downstream) | Compile-time after split |
| **Downstream usage** | `event.getAfter().getOrigin()` | `objectMapper.readTree(event.getAfter()).get("origin")` | `order.getCustomerId()` (after split) |
| **Best for** | 1-3 tables with complex business logic | Many tables, generic pipelines (mirroring) | Multiple tables with different business logic per table |

## Key Concepts Deep Dive

### 1. Why `Serializable`?

```java
public class JsonCdcDeserializer implements DebeziumDeserializationSchema<CdcEvent> {
    private static final long serialVersionUID = 1L;
```

Flink ships your deserializer from the JobManager to TaskManagers over the network. Java serialization converts the object to bytes for transport. Every class that travels across the network must implement `Serializable`.

`serialVersionUID` is a version number — if you change the class, bumping this value tells Java "the old serialized form is incompatible, don't try to deserialize it." If you don't declare it, Java generates one from the class structure, which can cause mysterious failures after minor code changes.

### 2. Why `transient` on ObjectMapper?

```java
private transient ObjectMapper objectMapper;  // excluded from serialization
```

`ObjectMapper` is not serializable (it holds thread pools, caches, etc.). Marking it `transient` means Java skips it during serialization. After the deserializer arrives at the TaskManager and is deserialized, `objectMapper` is `null`. The lazy getter recreates it on first use:

```java
private ObjectMapper getObjectMapper() {
    if (objectMapper == null) {             // null after deserialization
        objectMapper = new ObjectMapper();  // recreate on TaskManager
    }
    return objectMapper;
}
```

This is a standard Flink pattern for non-serializable resources. You'll see it with database connections, HTTP clients, etc.

**Lifecycle visualization:**

```
JobManager                          TaskManager
┌──────────────────┐   serialize   ┌──────────────────┐
│ objectMapper = OM │ ──────────►  │ objectMapper = null│  (transient field lost)
└──────────────────┘               └────────┬─────────┘
                                            │ first deserialize() call
                                            ▼
                                   ┌──────────────────┐
                                   │ objectMapper = OM │  (lazy init recreates it)
                                   └──────────────────┘
```

### 3. `Collector<T>` — How You Emit Records

```java
public void deserialize(SourceRecord record, Collector<CdcEvent> out) throws Exception {
    // ...build event...
    out.collect(event);   // emit into the DataStream
}
```

`Collector` is Flink's output mechanism. Calling `out.collect(event)` pushes one record into the downstream DataStream. You can:
- Call it **zero times** (filter out the record)
- Call it **once** (1:1 mapping, normal case)
- Call it **multiple times** (1:N fan-out, e.g. split one record into many)

This is the same `Collector` interface used in `FlatMapFunction` and other Flink operators.

### 4. `TypeInformation<T>` — Flink's Type System

```java
@Override
public TypeInformation<CdcEvent> getProducedType() {
    return TypeInformation.of(CdcEvent.class);
}
```

Flink doesn't rely on Java generics at runtime (they're erased). Instead, it uses `TypeInformation` to understand how to serialize, compare, and hash your objects. `TypeInformation.of(CdcEvent.class)` tells Flink to use its POJO serializer for `CdcEvent`.

Why does this matter? Flink uses this information to:
- Serialize records when shuffling between tasks (`keyBy`, `rebalance`)
- Store records in state (checkpoints, savepoints)
- Optimize memory layout for performance

If Flink can't infer the type (common with generics or lambdas), you get a `MissingTypeInfo` error at runtime — this method prevents that.

### 5. Schema-Driven vs Hardcoded Field Access

**Hardcoded (single-table):**
```java
// You must know every column name at compile time
shipment.setShipmentId(struct.getInt32("shipment_id"));
shipment.setOrderId(struct.getInt32("order_id"));
shipment.setOrigin(struct.getString("origin"));
```

**Schema-driven (multi-table):**
```java
// Iterates over whatever fields exist — works for any table
for (Field field : struct.schema().fields()) {
    String name = field.name();     // discovered at runtime
    Object val = struct.get(field); // generic access
}
```

`struct.schema()` returns a `Schema` object describing the Struct's fields. `schema().fields()` returns a `List<Field>`, where each `Field` has a `.name()` and a `.schema()` (the field's type). This is how the generic deserializer handles any table without knowing column names in advance.

### 6. MySQL CDC Type Gotcha: TINYINT(1) is NOT Boolean

MySQL's `BOOLEAN` is actually `TINYINT(1)`. Debezium sends it as `Short` (0 or 1), NOT Java `Boolean`:

```java
// What you might expect:
Boolean isArrived = struct.getBoolean("is_arrived");  // WRONG — throws exception

// What actually works:
Object isArrived = struct.get("is_arrived");          // returns Short(0) or Short(1)
```

This is why the single-table deserializer has special handling:

```java
Object isArrived = struct.get("is_arrived");
if (isArrived instanceof Boolean b)   s.setIsArrived(b);      // just in case
else if (isArrived instanceof Short v)  s.setIsArrived(v != 0); // the common case
else if (isArrived instanceof Integer v) s.setIsArrived(v != 0); // MySQL version variance
```

And why the generic deserializer doesn't need to care — it stores `Short(0)` as JSON number `0`, and the **sink** handles the boolean conversion when writing to Postgres.

## How to Wire It Up

```java
// In Main.java
MySqlSource<CdcEvent> source = MySqlSource.<CdcEvent>builder()
    .hostname("localhost")
    .port(3306)
    .databaseList("db_1")
    .tableList("db_1.shipments,db_1.orders")    // multiple tables
    .username("mysqluser")
    .password("mysqlpw")
    .serverId("7100-7104")
    .deserializer(new JsonCdcDeserializer())     // ← your deserializer here
    .startupOptions(StartupOptions.latest())
    .build();

DataStream<CdcEvent> cdcStream = env
    .fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL Source")
    .setParallelism(1);
```

The `.deserializer()` builder method is where you plug in your implementation. The generic type `<CdcEvent>` on `MySqlSource.<CdcEvent>builder()` must match the type your deserializer produces.

## Decision Flowchart: Which Approach to Use?

```
Do you need to listen to multiple tables?
│
├── No, just 1-2 tables
│   └── Do you need compile-time type safety on the CDC fields?
│       ├── Yes → Approach 1: Single-Table POJO deserializer
│       └── No  → Either approach works, pick simpler one
│
└── Yes, 3+ tables
    └── Do all tables go through the same generic pipeline (e.g. mirroring)?
        ├── Yes → Approach 2: Generic JSON deserializer (one class for all)
        └── No, different business logic per table
            └── Approach 3: Hybrid — Generic JSON source + typed split downstream
                ├── 2-3 typed tables → Option A: filter() + map() to POJOs
                └── 4+ typed tables → Option B: Side outputs (single pass)
```

## TL;DR

| Question | Answer |
|----------|--------|
| **What is it?** | Interface to convert Debezium `SourceRecord` → your Java type |
| **How many methods?** | 2: `deserialize()` and `getProducedType()` |
| **Input format** | Kafka Connect `Struct` (typed, schema-attached, NOT JSON) |
| **Key gotcha** | MySQL `BOOLEAN` arrives as `Short(0/1)`, not Java `Boolean` |
| **Single-table** | Map Struct fields to POJO (type-safe, one class per table) |
| **Multi-table** | Convert Struct to JSON string (generic, one class for all) |
| **Hybrid** | Generic JSON source → split into typed POJOs downstream (best of both) |
| **Must be Serializable** | Yes — Flink ships it to TaskManagers over the network |
| **Non-serializable fields** | Mark `transient`, recreate lazily after deserialization |

---

## Takeaways

### What You Should Learn From This Doc

1. **Debezium doesn't give you JSON** — it gives you Kafka Connect `Struct` objects with attached schemas. Understanding this prevents confusion when reading deserializer code
2. **Two deserialization strategies exist** — single-table POJO (type-safe, one class per table) vs multi-table JSON (generic, one class for all tables). Know when to pick which
3. **The `transient` + lazy init pattern** — any non-serializable resource (ObjectMapper, DB connections) in a Flink operator must be `transient` and recreated after deserialization. This pattern appears everywhere in Flink
4. **TINYINT(1) arrives as Short, not Boolean** — this single gotcha causes runtime failures across Postgres and Snowflake sinks. Recognizing it saves hours of debugging

### How This Helps You Understand the Flink Application

- You can now read `JsonCdcDeserializer.java` and understand why it iterates `Struct.schema().fields()` instead of hardcoding field names
- You understand why `ShipmentDebeziumDeserializer.java` (V0) was replaced — it only handles one table
- You see why `CdcEvent` carries raw JSON strings instead of typed fields — it's the multi-table generic approach

### Other Benefits of This Knowledge

- **Build connectors for other CDC sources** — Debezium supports Postgres, MongoDB, SQL Server, Oracle. The same `DebeziumDeserializationSchema` pattern works for all of them
- **Write custom Flink deserializers** — the `Serializable` + `transient` + `TypeInformation` pattern applies to any Flink deserialization interface (e.g., `KafkaDeserializationSchema`)
- **Debug CDC pipelines** — when data looks wrong downstream, you can trace it back to the Struct and know exactly what Debezium sent vs what your deserializer produced
