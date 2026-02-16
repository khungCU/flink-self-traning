# Jackson ObjectMapper & JsonNode - Quick Lesson

## What is Jackson?

Jackson is the **standard JSON library for Java**. It lets you convert between Java objects and JSON strings. Almost every Java project that deals with JSON uses Jackson — Spring Boot, Flink, Elasticsearch, Kafka, etc.

The core dependency:

```groovy
implementation 'com.fasterxml.jackson.core:jackson-databind:2.15.0'
```

This single dependency gives you `ObjectMapper`, `JsonNode`, `ObjectNode`, and everything below.

## The Problem It Solves

Java has no built-in JSON support. If you receive a JSON string like:

```json
{"shipment_id": 1, "origin": "Shanghai", "is_arrived": false}
```

You need a way to:
1. **Read** it — extract `"origin"` as a Java `String`, `"shipment_id"` as an `int`
2. **Write** it — convert a Java object back to a JSON string
3. **Build** it — construct JSON from scratch (e.g. from database column values)

Jackson provides all three through one central class: `ObjectMapper`.

## ObjectMapper — The Engine

`ObjectMapper` is the entry point for all JSON operations. Think of it as a **translator** between Java and JSON.

```java
ObjectMapper mapper = new ObjectMapper();  // create once, reuse everywhere
```

It does three things:

### 1. Java Object → JSON String (Serialization)

```java
// POJO to JSON
Shipment s = new Shipment(1, 100, "Shanghai", "Tokyo", false);
String json = mapper.writeValueAsString(s);
// → '{"shipmentId":1,"orderId":100,"origin":"Shanghai","destination":"Tokyo","isArrived":false}'
```

### 2. JSON String → Java Object (Deserialization)

```java
// JSON to POJO
String json = "{\"shipmentId\":1,\"origin\":\"Shanghai\"}";
Shipment s = mapper.readValue(json, Shipment.class);
s.getOrigin();  // → "Shanghai"
```

### 3. JSON String → Tree (Parsing without a POJO)

```java
// JSON to tree (when you don't have or don't want a POJO)
String json = "{\"shipment_id\":1,\"origin\":\"Shanghai\",\"is_arrived\":false}";
JsonNode root = mapper.readTree(json);
root.get("origin").textValue();      // → "Shanghai"
root.get("shipment_id").intValue();  // → 1
root.get("is_arrived").booleanValue(); // → false
```

### All three visualized:

```
                    ObjectMapper
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
  writeValueAsString   readValue      readTree
   (Java → JSON)    (JSON → POJO)  (JSON → Tree)
                                        │
        ┌───────────────────────────────┘
        ▼
     JsonNode (read-only tree)
        │
        ├── .get("field")        → JsonNode
        ├── .textValue()         → String
        ├── .intValue()          → int
        ├── .booleanValue()      → boolean
        └── .fields()            → iterate all key-value pairs
```

## Why Not Just Use String Operations?

You could try to parse JSON manually:

```java
// DON'T DO THIS
String origin = json.substring(json.indexOf("\"origin\":\"") + 10);
origin = origin.substring(0, origin.indexOf("\""));
```

This is fragile, error-prone, and breaks when:
- Field order changes
- Values contain quotes or special characters (`"origin": "O\"Hare"`)
- Fields are null (`"origin": null`)
- Numbers have decimals (`"price": 10.50`)

Jackson handles all these edge cases correctly.

## The Three Ways to Work with JSON

Jackson gives you three approaches. Each fits different situations:

```
Approach          When to use                       Example
─────────────────────────────────────────────────────────────────
1. POJO binding   You know the structure at          readValue(json, Shipment.class)
                  compile time, want type safety

2. Tree model     You DON'T know the structure,      readTree(json) → JsonNode
   (JsonNode)     or it varies per record

3. Manual build   You're CREATING JSON from           createObjectNode() → ObjectNode
   (ObjectNode)   non-JSON data (e.g. Struct)
─────────────────────────────────────────────────────────────────
```

### Approach 1: POJO Binding (readValue / writeValueAsString)

Best when you know exactly what the JSON looks like.

```java
// You need a POJO class
public class Shipment {
    private int shipmentId;
    private String origin;
    // getters + setters required for Jackson
}

// JSON → POJO
Shipment s = mapper.readValue(json, Shipment.class);
s.getOrigin();     // compile-time safe, IDE autocomplete works

// POJO → JSON
String json = mapper.writeValueAsString(s);
```

**Used in this project:** `ShipmentCdcEvent` model — the single-table deserializer maps Struct to this typed POJO.

### Approach 2: Tree Model / JsonNode (readTree)

Best when the JSON structure varies or you don't want to create a POJO.

```java
JsonNode root = mapper.readTree("{\"name\":\"Alice\",\"age\":30}");

// Navigate fields
root.get("name").textValue();    // → "Alice"
root.get("age").intValue();      // → 30
root.get("missing");             // → null (no exception)
root.get("missing").textValue(); // → NullPointerException! Check for null first.

// Check types before accessing
JsonNode age = root.get("age");
age.isInt();        // → true
age.isTextual();    // → false
age.isNull();       // → false

// Iterate all fields (key-value pairs)
root.fields().forEachRemaining(entry -> {
    String key = entry.getKey();      // "name", "age"
    JsonNode val = entry.getValue();  // JsonNode for each value
});
```

**Used in this project:** `PGSinker` uses `readTree()` to parse the JSON strings stored in `CdcEvent.getAfter()` and `CdcEvent.getBefore()`, then iterates all fields to dynamically build SQL.

### Approach 3: ObjectNode (createObjectNode — building JSON)

Best when you're constructing JSON from non-JSON data.

```java
ObjectNode node = mapper.createObjectNode();    // {}

node.put("shipment_id", 1);                     // {"shipment_id": 1}
node.put("origin", "Shanghai");                 // {"shipment_id": 1, "origin": "Shanghai"}
node.put("is_arrived", false);                  // {"shipment_id": 1, "origin": "Shanghai", "is_arrived": false}
node.putNull("destination");                    // {"shipment_id": 1, ..., "destination": null}

String json = mapper.writeValueAsString(node);
// → '{"shipment_id":1,"origin":"Shanghai","is_arrived":false,"destination":null}'
```

**Used in this project:** `JsonCdcDeserializer.structToJson()` uses `ObjectNode` to build a JSON string from the Debezium `Struct` — because `Struct` is not JSON, we create an `ObjectNode`, add each column as a key-value pair, then serialize to a JSON string.

## JsonNode vs ObjectNode

These two are related but serve different purposes:

```
                  JsonNode (abstract base class)
                     │
                     │  read-only: .get(), .textValue(), .intValue(), .fields()
                     │
          ┌──────────┴──────────┐
          │                     │
     ObjectNode              ArrayNode
     (JSON object)           (JSON array)
     { "key": "val" }       [1, 2, 3]
          │
          │  adds write methods:
          │  .put(), .set(), .putNull(), .remove()
```

| | JsonNode | ObjectNode |
|---|---|---|
| **What is it** | Read-only JSON tree node | Mutable JSON object node |
| **Can read?** | Yes — `.get()`, `.textValue()`, etc. | Yes (inherits from JsonNode) |
| **Can write?** | No | Yes — `.put()`, `.set()`, `.putNull()` |
| **How to get one** | `mapper.readTree(json)` | `mapper.createObjectNode()` |
| **Typical use** | Parse existing JSON | Build new JSON from scratch |

**Simple rule:** Use `JsonNode` when reading, `ObjectNode` when writing.

## Complete Example: How This Project Uses Both

Here's the full data flow showing where each is used:

```
MySQL Row                    JsonCdcDeserializer              CdcEvent               PGSinker
─────────                    ───────────────────              ────────               ────────

shipment_id = 1         ┌─► Struct (Kafka Connect)
origin = "Shanghai"     │       │
is_arrived = 0 (Short)  │       │  ObjectNode: build JSON
                        │       │  from Struct fields
  MySQL binlog ─────────┘       ▼
                           ObjectNode node = createObjectNode()
                           node.put("shipment_id", 1)
                           node.put("origin", "Shanghai")
                           node.put("is_arrived", 0)           ─► after = '{"shipment_id":1,...}'
                           writeValueAsString(node)                │
                                                                   │
                                                              CdcEvent stores
                                                              JSON as String
                                                                   │
                                                                   ▼
                                                              JsonNode: parse JSON
                                                              back to extract values
                                                                   │
                                                              readTree(after)
                                                              row.get("shipment_id").intValue() → 1
                                                              row.get("origin").textValue() → "Shanghai"
                                                              row.fields() → iterate for SQL
                                                                   │
                                                                   ▼
                                                              INSERT INTO "shipments"
                                                              ("shipment_id","origin","is_arrived")
                                                              VALUES (1, 'Shanghai', false)
```

**Step 1 — Deserializer uses ObjectNode (write):**
```java
// In JsonCdcDeserializer.structToJson()
ObjectNode node = getObjectMapper().createObjectNode();
for (Field field : struct.schema().fields()) {
    node.put(field.name(), struct.get(field));     // build JSON object
}
return getObjectMapper().writeValueAsString(node);  // → JSON string
```

**Step 2 — CdcEvent carries JSON as plain String:**
```java
// CdcEvent just stores it
private String after;  // '{"shipment_id":1,"origin":"Shanghai","is_arrived":0}'
```

**Step 3 — PGSinker uses JsonNode (read):**
```java
// In PostgresWriter.executeUpsert()
JsonNode row = objectMapper.readTree(json);         // parse JSON string → tree
row.fields().forEachRemaining(entry -> {
    columns.add(entry.getKey());                     // column names for SQL
    values.add(extractValue(entry.getValue()));      // column values for PreparedStatement
});
```

## extractValue: JsonNode → Java Types

When building SQL with `PreparedStatement`, you need actual Java types (`int`, `String`, `boolean`), not `JsonNode`. The `extractValue()` method in `PGSinker` converts:

```java
private Object extractValue(JsonNode node) {
    if (node == null || node.isNull()) return null;

    return switch (node.getNodeType()) {
        case NUMBER  -> node.isInt()    ? node.intValue()
                      : node.isLong()   ? node.longValue()
                      : node.isFloat()  ? node.floatValue()
                      : node.doubleValue();
        case BOOLEAN -> node.booleanValue();
        case STRING  -> node.textValue();
        default      -> node.asText();        // fallback: toString
    };
}
```

Why not just use `node.asText()` for everything? Because `PreparedStatement.setObject()` needs the correct Java type to map to the right SQL type. If you pass `"1"` (String) for an `INT` column, Postgres may reject it or silently cast it wrong.

**JsonNode type check methods:**

| Method | True when | Extract with |
|--------|-----------|-------------|
| `isInt()` | JSON number fits in int | `intValue()` |
| `isLong()` | JSON number fits in long | `longValue()` |
| `isFloat()` | JSON number has decimal | `floatValue()` |
| `isDouble()` | JSON number (any) | `doubleValue()` |
| `isTextual()` | JSON string | `textValue()` |
| `isBoolean()` | JSON true/false | `booleanValue()` |
| `isNull()` | JSON null | return Java `null` |
| `isArray()` | JSON array `[...]` | iterate with `elements()` |
| `isObject()` | JSON object `{...}` | iterate with `fields()` |

## ObjectMapper: Create Once, Reuse

`ObjectMapper` is **thread-safe** and relatively expensive to create (it initializes serializers, caches, etc.). Always create it once and reuse:

```java
// GOOD: create once
private final ObjectMapper mapper = new ObjectMapper();

public void process(String json1, String json2) {
    JsonNode a = mapper.readTree(json1);  // reuse
    JsonNode b = mapper.readTree(json2);  // reuse
}

// BAD: creates a new ObjectMapper per call
public void process(String json) {
    ObjectMapper mapper = new ObjectMapper();  // wasteful!
    JsonNode node = mapper.readTree(json);
}
```

In Flink, since `ObjectMapper` is not serializable, you use the `transient` + lazy init pattern (covered in the [Debezium deserializer lesson](./debezium-deserializer-lesson.md)):

```java
private transient ObjectMapper objectMapper;   // not serialized across network

private ObjectMapper getObjectMapper() {
    if (objectMapper == null) {
        objectMapper = new ObjectMapper();     // recreated on TaskManager
    }
    return objectMapper;
}
```

## Common Patterns Cheat Sheet

```java
ObjectMapper mapper = new ObjectMapper();

// ── Read JSON ──────────────────────────────────────────────
// Parse to tree
JsonNode root = mapper.readTree("{\"name\":\"Alice\",\"age\":30}");
root.get("name").textValue();       // "Alice"
root.get("age").intValue();         // 30

// Parse to POJO
User user = mapper.readValue(json, User.class);

// Parse to Map (loses type nuance — numbers may all become Double)
Map<String, Object> map = mapper.readValue(json, Map.class);

// ── Write JSON ─────────────────────────────────────────────
// POJO to JSON string
String json = mapper.writeValueAsString(user);

// Build from scratch
ObjectNode node = mapper.createObjectNode();
node.put("name", "Alice");
node.put("age", 30);
node.putNull("email");
String json = mapper.writeValueAsString(node);  // '{"name":"Alice","age":30,"email":null}'

// ── Nested objects ─────────────────────────────────────────
ObjectNode address = mapper.createObjectNode();
address.put("city", "Shanghai");
node.set("address", address);       // {"name":"Alice", "address":{"city":"Shanghai"}}

// ── Arrays ─────────────────────────────────────────────────
ArrayNode tags = mapper.createArrayNode();
tags.add("vip");
tags.add("new");
node.set("tags", tags);             // {"name":"Alice", "tags":["vip","new"]}

// ── Check and navigate ─────────────────────────────────────
root.has("name");                    // true — field exists
root.has("missing");                 // false
root.path("missing").isMissingNode(); // true (safer than .get() which returns null)
root.get("missing");                  // null (NullPointerException if you chain .textValue())
```

## TL;DR

| Question | Answer |
|----------|--------|
| **What is ObjectMapper?** | Jackson's central class — translates between Java objects and JSON |
| **What is JsonNode?** | Read-only in-memory representation of parsed JSON |
| **What is ObjectNode?** | Mutable subclass of JsonNode — for building JSON from scratch |
| **When to use readTree?** | When you need to inspect JSON without creating a POJO (dynamic/unknown structure) |
| **When to use readValue?** | When you have a POJO class and want type-safe deserialization |
| **When to use ObjectNode?** | When you're constructing JSON from non-JSON data (e.g. database rows, Structs) |
| **Thread-safe?** | Yes — create one `ObjectMapper`, reuse across threads |
| **In Flink?** | Mark `transient`, recreate lazily (not serializable) |

---

## Takeaways

### What You Should Learn From This Doc

1. **ObjectMapper is the central engine** — one class handles all of: POJO to JSON, JSON to POJO, JSON to tree, tree to JSON. You don't need multiple libraries
2. **Three approaches exist** — POJO binding (`readValue`/`writeValueAsString`), tree model (`readTree` → `JsonNode`), and manual build (`ObjectNode`). Pick based on whether your schema is known at compile time
3. **JsonNode is read-only, ObjectNode is mutable** — `readTree()` gives you `JsonNode` for reading; `createObjectNode()` gives you `ObjectNode` for building. ObjectNode extends JsonNode
4. **ObjectMapper is NOT serializable** — in Flink, mark it `transient` and recreate lazily. This is a common gotcha that causes `NotSerializableException` at deploy time

### How This Helps You Understand the Flink Application

- You can now read `JsonCdcDeserializer.java` and understand why it uses `ObjectNode.put()` to manually build JSON from Struct fields (there's no POJO for the generic multi-table approach)
- You can read `PGSinker.java` and understand why it uses `objectMapper.readTree(json)` to parse the JSON back into a `JsonNode` tree for dynamic column extraction
- You see the symmetry: deserializer builds JSON with ObjectNode → sinker reads it back with readTree

### Other Benefits of This Knowledge

- **Jackson is everywhere in Java** — Spring Boot, Kafka, Elasticsearch, AWS SDK all use Jackson internally. Mastering it here applies across your entire Java career
- **Handle any API response** — when consuming REST APIs, use `readTree()` for dynamic JSON or `readValue()` for typed responses
- **Build data transformations** — Jackson's tree model lets you transform JSON structures (add fields, rename keys, flatten nested objects) without creating intermediate POJOs
- **Stream large JSON files** — Jackson's `JsonParser` (streaming API) can process gigabyte-scale JSON files without loading them into memory
