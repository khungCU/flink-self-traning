# getColumnTypes() — Quick Lesson

## What Is It?

`getColumnTypes()` is a method in `PGSinker` / `SFSinker` that queries the **destination database** to discover the column types of a table at runtime. It answers the question: "What type is column `is_arrived` in Postgres?" → `"boolean"`.

```java
private Map<String, String> getColumnTypes(String table) throws SQLException {
    String sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?";
    // ... execute and return map like:
    // {"shipment_id": "integer", "origin": "character varying", "is_arrived": "boolean"}
}
```

## Why Do We Need It?

Because of a **type mismatch** between what MySQL CDC sends and what the destination database expects.

The specific problem:

```
MySQL table:    is_arrived TINYINT(1)        ← MySQL's way of saying BOOLEAN
                    │
                    ▼
Debezium CDC:   is_arrived = Short(0)        ← Debezium sends TINYINT as Short (Java)
                    │
                    ▼
JSON in CdcEvent: "is_arrived": 0            ← serialized as a number
                    │
                    ▼
Jackson readTree: node.intValue() → 0        ← extracted as Integer
                    │
                    ▼
PreparedStatement: ps.setObject(3, 0)        ← sends Integer 0 to Postgres
                    │
                    ▼
Postgres:         column "is_arrived" is of type boolean
                  but expression is of type integer   ← BOOM! PSQLException
```

The fix: before calling `ps.setObject()`, check what type Postgres expects, and convert if needed:

```
getColumnTypes("shipments") → {"is_arrived": "boolean", ...}
                                      │
                                      ▼
convertValue(0, "boolean") → false    ← Integer 0 converted to Boolean false
                                      │
                                      ▼
ps.setObject(3, false)                ← sends Boolean to Postgres ← works!
```

**Without `getColumnTypes()`**, we'd have no way of knowing that Postgres expects a `boolean` for `is_arrived` — because the CDC event just says it's a number.

## How Does It Work?

### Step 1: Query information_schema

Every relational database has an `information_schema` — a standard set of read-only views that describe the database structure. The `columns` view lists every column of every table with its data type.

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'shipments';
```

Returns:

```
 column_name  | data_type
--------------+-------------------
 shipment_id  | integer
 order_id     | integer
 origin       | character varying
 destination  | character varying
 is_arrived   | boolean
```

### Step 2: Cache the Result

The result is stored in a `HashMap` so we only query once per table:

```java
private final Map<String, Map<String, String>> columnTypeCache = new HashMap<>();
//                  ↑               ↑          ↑
//             table name     column name   data type
//           "shipments"    "is_arrived"   "boolean"
```

### Step 3: Use in convertValue()

Every time we set a value on a `PreparedStatement`, we look up the target type and convert if needed:

```java
String pgType = colTypes.get(columns.get(i));          // "boolean"
ps.setObject(i + 1, convertValue(values.get(i), pgType)); // 0 → false
```

## When Does the Query Run?

**Once per table, on the first CDC event for that table.** Not at application startup, not per event.

```
Timeline:
─────────────────────────────────────────────────────────────

Event 1: table="shipments"
  → getColumnTypes("shipments")
  → cache miss → query information_schema   ← DB query (only time!)
  → cache: {"shipments": {"shipment_id":"integer", "is_arrived":"boolean"}}
  → executeMerge / executeUpsert

Event 2: table="shipments"
  → getColumnTypes("shipments")
  → cache hit → return cached map           ← no DB query
  → executeMerge / executeUpsert

Event 3: table="orders"
  → getColumnTypes("orders")
  → cache miss → query information_schema   ← DB query (first time for "orders")
  → cache: {"shipments": {...}, "orders": {"order_id":"integer", ...}}

Event 4: table="shipments"
  → cache hit                               ← no DB query

Event 5: table="orders"
  → cache hit                               ← no DB query

...event 6 through 1,000,000...
  → all cache hits                          ← no DB queries ever again
```

## Where Is the Cache Stored?

The cache lives **inside each `SinkWriter` instance** as a plain Java `HashMap`. It is NOT Flink managed state — it doesn't go into checkpoints.

```
TaskManager 1                          TaskManager 2
┌─────────────────────────┐           ┌─────────────────────────┐
│ PostgresWriter #1       │           │ PostgresWriter #2       │
│                         │           │                         │
│ columnTypeCache:        │           │ columnTypeCache:        │
│   "shipments" → {...}   │           │   "orders" → {...}      │
│                         │           │                         │
│ (queried once when      │           │ (queried once when      │
│  first shipments event  │           │  first orders event     │
│  arrived)               │           │  arrived)               │
└─────────────────────────┘           └─────────────────────────┘
```

With `keyBy(CdcEvent::getTable)`:
- Writer #1 gets all `shipments` events → queries `shipments` column types once
- Writer #2 gets all `orders` events → queries `orders` column types once
- Each writer only caches the tables it handles

Without `keyBy`:
- Each writer might see events from any table → each writer queries and caches all tables it encounters

## Performance Impact

### Cost: Almost Zero

| Aspect | Impact |
|--------|--------|
| **How many queries?** | 1 per table per writer instance. For 50 tables with parallelism 4: worst case 200 queries total over the entire job lifetime |
| **When do they run?** | Lazily, on first event. Spread over time as new tables appear |
| **Query cost** | `information_schema` is metadata — Postgres/Snowflake serves it from catalog cache, typically < 1ms |
| **Cache size** | ~50 bytes per column × ~10 columns × 50 tables = ~25KB. Negligible |
| **Ongoing cost** | Zero after warm-up. Every subsequent event is a `HashMap.get()` (~nanoseconds) |

### What If Schema Changes?

Since the cache lives for the lifetime of the writer (the entire Flink job), it does **not** pick up schema changes automatically:

```
1. Job starts, caches shipments: {shipment_id: integer, origin: varchar, is_arrived: boolean}
2. DBA adds column: ALTER TABLE shipments ADD COLUMN weight DOUBLE
3. CDC event arrives with "weight": 10.5
4. getColumnTypes() returns cached version — no "weight" entry
5. convertValue(10.5, null) → returns 10.5 as-is (pgType is null, no conversion)
6. ps.setObject(4, 10.5) → works fine for most types, Postgres infers DOUBLE
```

**Adding a column:** Usually works — `convertValue` returns the value as-is when `pgType` is null. The JDBC driver infers the type from the Java object.

**Changing a column type:** Might fail — the cached type is stale. For example, changing `is_arrived` from `BOOLEAN` to `INTEGER` would still apply the boolean conversion.

**Fix:** Restart the Flink job to clear the cache. Or add a TTL-based eviction (not implemented — keep it simple for now).

## Could We Skip getColumnTypes() Entirely?

What happens if we remove the type lookup and just do `ps.setObject(i, value)` without conversion?

| Scenario | Without getColumnTypes | With getColumnTypes |
|----------|----------------------|---------------------|
| INT → INTEGER | Works (Java Integer → SQL INTEGER) | Works |
| VARCHAR → VARCHAR | Works (Java String → SQL VARCHAR) | Works |
| **TINYINT(1) → BOOLEAN** | **FAILS** (Java Integer 0 → SQL BOOLEAN = type mismatch) | Works (Integer 0 → Boolean false) |
| DOUBLE → DOUBLE | Works | Works |
| BIGINT → BIGINT | Works | Works |

Most types work fine without conversion. The method exists **almost entirely** for the `BOOLEAN` edge case. If all your columns were INT/VARCHAR/DOUBLE, you wouldn't need it.

But since MySQL's `TINYINT(1)` → `BOOLEAN` mismatch is extremely common (almost every table has a boolean flag), the method pays for itself.

## Alternative Approaches

Instead of querying `information_schema`, you could:

| Approach | Pros | Cons |
|----------|------|------|
| **Query information_schema (current)** | Automatic, no config needed | Extra DB query per table (once) |
| **Hardcode type overrides in config** | No DB queries at all | Manual maintenance for every table |
| **Read from Debezium schema** | Available in the `SourceRecord` | Schema.Type for TINYINT(1) is `INT16`, not `BOOLEAN` — same problem |
| **Try/catch and retry with conversion** | No upfront query | Slower on first failure, messy error handling |

The `information_schema` approach is the best trade-off: automatic, negligible cost, and accurate because it reads from the **destination** database (which is where the type must match).

## Code Walkthrough

```java
private Map<String, String> getColumnTypes(String table) throws SQLException {
    // table arrives quoted: "\"shipments\"" → strip to "shipments"
    String rawTable = table.replace("\"", "");

    // Check cache first — O(1) HashMap lookup
    if (columnTypeCache.containsKey(rawTable)) {
        return columnTypeCache.get(rawTable);
    }

    // Cache miss: query the database
    Map<String, String> types = new HashMap<>();
    String sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?";

    // Uses its OWN connection from the pool (not the batch transaction connection)
    // because this is a read-only metadata query, independent of the CDC writes
    try (Connection conn = dataSource.getConnection();
         PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, rawTable);
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                types.put(rs.getString("column_name"), rs.getString("data_type"));
            }
        }
    }

    // Store in cache — all future calls for this table skip the DB query
    columnTypeCache.put(rawTable, types);
    return types;
}
```

**Note:** `getColumnTypes()` gets its own connection from the pool, separate from the batch transaction connection in `flush()`. This is because the metadata query is a read-only operation that doesn't need to be part of the CDC write transaction.

## TL;DR

| Question | Answer |
|----------|--------|
| **What does it do?** | Queries the destination DB for column types of a table |
| **Why?** | To fix type mismatches (MySQL `TINYINT(1)` → number → needs boolean conversion) |
| **When does it query?** | Once per table, on the first CDC event for that table |
| **Where is the cache?** | In-memory `HashMap` inside each `SinkWriter` instance (not Flink state) |
| **Performance impact?** | Negligible — 1 metadata query per table for the entire job lifetime |
| **Cache size for 50 tables?** | ~25KB — trivial |
| **Picks up schema changes?** | No — restart the job to refresh. Adding columns usually works anyway |
| **Could we skip it?** | Most types work without it. Exists mainly for the `BOOLEAN` edge case |
