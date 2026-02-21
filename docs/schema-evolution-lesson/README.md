# Schema Evolution in Flink CDC Pipelines - Risk Analysis & Mitigation

## What is Schema Evolution?

Schema evolution (also called DDL evolution or schema change propagation) is what happens when the **source database schema changes** while a CDC pipeline is running. In a sync pipeline (Source A → Destination B), the question is: **does an upstream DDL change break the downstream sync?**

```
MySQL (schema changes here)  →  Flink CDC Pipeline  →  Postgres (must stay in sync)
       ALTER TABLE ...              does it break?          still working?
```

This is a critical concern for any production CDC pipeline because databases schemas change regularly — new columns, renamed fields, type migrations, dropped columns.

## Our Pipeline Architecture

```
MySqlSource → JsonCdcDeserializer → CdcEvent → keyBy(table) → PGSinker → Postgres
```

Where schema knowledge lives at each layer:

| Layer | Schema Knowledge Source |
|-------|------------------------|
| MySqlSource (Debezium) | MySQL binlog + in-memory schema history |
| JsonCdcDeserializer | `Struct.schema().fields()` (runtime, from Debezium) |
| CdcEvent | None — opaque carrier (JSON strings) |
| PGSinker | 1) JSON keys from event payload 2) `columnTypeCache` from Postgres `information_schema` |
| Postgres table | The `CREATE TABLE` DDL |

**Key insight:** There is no single source of truth for schema. The source schema (MySQL), the transport schema (Debezium Struct), and the destination schema (Postgres) can all diverge independently.

## The Four DDL Change Scenarios

### Scenario 1: ADD a New Column

Example: `ALTER TABLE shipments ADD COLUMN weight DOUBLE;`

**Layer-by-layer trace:**

| Layer | What Happens | Status |
|-------|-------------|--------|
| MySqlSource (Debezium) | Updates internal schema history. New events include `weight` in the `after` Struct | OK |
| JsonCdcDeserializer | `structToJson()` iterates `struct.schema().fields()` — discovers `weight` at runtime | OK |
| CdcEvent | `after` JSON now contains `"weight": 10.5` — it's just a string carrier | OK |
| PGSinker SQL generation | `executeUpsert()` builds SQL from JSON keys — `weight` appears in INSERT column list | OK |
| columnTypeCache | `colTypes.get("weight")` returns `null` (stale cache). `convertValue()` returns raw value as-is — JDBC infers the type | Safe fallback |
| **Postgres table** | **If `weight` column doesn't exist → INSERT fails → transaction rollback → infinite retry loop** | **BREAKS** |

```
Timeline of failure:

MySQL: ALTER TABLE shipments ADD COLUMN weight DOUBLE;
  ↓
Debezium picks up DDL, new events have "weight"
  ↓
PGSinker generates: INSERT INTO "shipments" (..., "weight") VALUES (..., ?)
  ↓
Postgres: ERROR: column "weight" of relation "shipments" does not exist
  ↓
flush() fails → conn.rollback() → Flink restores from checkpoint → same event replays → infinite loop
```

**Risk level: MEDIUM.** Pipeline adapts automatically, but Postgres must have the column FIRST.

---

### Scenario 2: DELETE a Column

Example: `ALTER TABLE shipments DROP COLUMN destination;`

**Layer-by-layer trace:**

| Layer | What Happens | Status |
|-------|-------------|--------|
| MySqlSource (Debezium) | Updates schema. Subsequent events no longer contain `destination` | OK |
| JsonCdcDeserializer | One fewer field in iteration. Shorter JSON output | OK |
| CdcEvent | Smaller JSON string | OK |
| PGSinker SQL generation | SQL no longer references `destination` — built dynamically per event | OK |
| columnTypeCache | Stale entry `"destination" -> "character varying"` exists but never looked up again | Harmless |
| Postgres table | Missing column filled with `DEFAULT` or `NULL`. Fails only if `NOT NULL` constraint without default | Minor risk |

**Subtle ordering detail:** During the transition window, events from **before** the DDL still have `destination`, while events **after** don't. Both arrive in the same buffer at `flush()`. This works correctly because each event generates its own SQL from its own JSON payload.

**Risk level: LOW.** The pipeline adapts smoothly.

---

### Scenario 3: RENAME a Column

Example: `ALTER TABLE shipments RENAME COLUMN origin TO origin_city;`

This is the **most dangerous** scenario. A rename is effectively a DROP + ADD from every downstream system's perspective.

**Layer-by-layer trace:**

| Layer | What Happens | Status |
|-------|-------------|--------|
| MySqlSource (Debezium) | Detects rename via DDL parsing. Subsequent Structs have `origin_city` instead of `origin` | OK |
| JsonCdcDeserializer | JSON output now contains `"origin_city"` instead of `"origin"` | OK |
| CdcEvent | JSON string has different key name | OK |
| PGSinker SQL generation | SQL now references `origin_city` | OK (if Postgres has it) |
| columnTypeCache | Stale: has `"origin"` but not `"origin_city"`. Null fallback is safe for VARCHAR | Stale but safe |
| **Postgres table** | **Column `origin_city` doesn't exist → INSERT fails → infinite retry loop** | **BREAKS** |

**The unsolvable ordering problem:**

```
Option A: Rename MySQL first, then Postgres
  MySQL: origin → origin_city     ← events now have "origin_city"
  Window of failure: events arrive with "origin_city" but Postgres still has "origin"
  Postgres: origin → origin_city  ← too late, events already failed

Option B: Rename Postgres first, then MySQL
  Postgres: origin → origin_city  ← destination ready
  Window of failure: events still arrive with "origin" but Postgres now has "origin_city"
  MySQL: origin → origin_city     ← too late, in-flight events already failed
```

There is **no safe ordering** without pausing the pipeline.

**Risk level: HIGH.** Requires coordinated cutover with pipeline pause.

---

### Scenario 4: CHANGE a Column's Data Type

Example: `ALTER TABLE shipments MODIFY COLUMN order_id BIGINT;` (INT → BIGINT)

**Layer-by-layer trace:**

| Layer | What Happens | Status |
|-------|-------------|--------|
| MySqlSource (Debezium) | Updates schema. Struct field `order_id` now reports as `INT64` | OK |
| JsonCdcDeserializer | `switch` handles both `Integer` and `Long` — both produce JSON numbers | OK |
| CdcEvent | JSON number, same key | OK |
| PGSinker SQL generation | SQL structure unchanged | OK |
| columnTypeCache | Stale type may cause wrong conversion in `convertValue()` | **Risk** |
| Postgres table | Type mismatch if destination not updated | **Risk** |

**The columnTypeCache problem — concrete example:**

```java
// In PGSinker.convertValue():
private Object convertValue(Object val, String pgType) {
    if (val == null || pgType == null) return val;
    if (pgType.equals("boolean") || pgType.equals("bool")) {
        // Converts numeric to boolean
        if (val instanceof Number n) return n.intValue() != 0;
    }
    return val;
}
```

**Dangerous case: `TINYINT(1)` → `INT` in MySQL**

- Cache still says `pgType = "boolean"` (from when it was TINYINT(1) mapped to Postgres boolean)
- MySQL now sends real integers like `42`
- `convertValue(42, "boolean")` → `42 != 0` → `true`
- **Silent data corruption** — real integer `42` becomes boolean `true`

**Widening (INT → BIGINT):** Generally safe. JDBC handles the conversion automatically.

**Narrowing or incompatible (VARCHAR → INT):** Postgres rejects the INSERT with a type error.

**Risk level: MEDIUM-HIGH**, depending on the specific type change.

## Risk Matrix Summary

| DDL Change | MySqlSource | Deserializer | CdcEvent | PGSinker SQL | columnTypeCache | Postgres Table | **Overall** |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **ADD column** | OK | OK | OK | OK | Safe fallback | **BREAKS** if missing | **MEDIUM** |
| **DROP column** | OK | OK | OK | OK | Harmless stale | Minor (NULL fill) | **LOW** |
| **RENAME column** | OK | OK | OK | **BREAKS** | Stale | **BREAKS** | **HIGH** |
| **CHANGE type** | OK | OK | OK | OK | **Wrong conversion** | **BREAKS** if incompatible | **MEDIUM-HIGH** |

## Why Our Pipeline is Both Resilient and Fragile

### Resilient Middle Layers

The generic design makes the middle layers remarkably adaptable:

1. **`structToJson()`** iterates `struct.schema().fields()` — discovers columns at runtime, no hardcoding
2. **`CdcEvent`** carries opaque JSON strings — never cares about field names or types
3. **`executeUpsert()`** builds SQL from JSON keys per event — adapts automatically

```
Source DDL change → Debezium adapts → Deserializer adapts → CdcEvent doesn't care → PGSinker adapts
                                                                                          ↓
                                                                                   Postgres doesn't adapt ← BREAK
```

### Fragile Edges

The fragility is concentrated at two points:

1. **Postgres destination schema** — never auto-updated. It's a static DDL that must be maintained manually
2. **`columnTypeCache`** — a write-once `HashMap` that never expires, never refreshes, and never detects stale entries

```java
// In PGSinker — the cache is populated once and never invalidated
private final Map<String, Map<String, String>> columnTypeCache = new HashMap<>();

private Map<String, String> getColumnTypes(String tableName) throws SQLException {
    if (columnTypeCache.containsKey(tableName)) {
        return columnTypeCache.get(tableName);  // returns stale data forever
    }
    // ... query information_schema once ...
}
```

## Mitigation: Schema Reconciliation Layer

### The Problem

Every DDL change risks breaking the sink because the pipeline blindly forwards whatever columns the source has, without checking what the destination expects.

### The Solution: Normalize Events Against Destination Schema

Add a **schema reconciliation layer** between `CdcEvent` and `PGSinker` that filters and normalizes events to match the destination's known schema.

```
MySqlSource → JsonCdcDeserializer → CdcEvent → [Schema Reconciliation] → PGSinker → Postgres
                                                         │
                                                    Side Output → Schema Drift Log
```

### How It Works Per Scenario

**RENAME (`origin` → `origin_city`):**

```
Incoming event JSON:  {"shipment_id": 1, "origin_city": "Tokyo", "is_arrived": true}
Destination schema:   [shipment_id, order_id, origin, destination, is_arrived]

Reconciliation:
  - "origin_city" not in destination schema → DROP it
  - "origin" in destination schema but missing from event → SET NULL
  - Other matching columns → PASS THROUGH

Output JSON:          {"shipment_id": 1, "origin": null, "is_arrived": true}
Side Output:          {table: "shipments", unknown_cols: ["origin_city"], missing_cols: ["origin"]}
```

**ADD (`weight` added to MySQL):**

```
Incoming event JSON:  {"shipment_id": 1, "origin": "Tokyo", "weight": 10.5}
Destination schema:   [shipment_id, order_id, origin, destination, is_arrived]

Reconciliation:
  - "weight" not in destination schema → DROP it

Output JSON:          {"shipment_id": 1, "origin": "Tokyo"}
Side Output:          {table: "shipments", unknown_cols: ["weight"]}
```

**DROP (`destination` removed from MySQL):**

```
Incoming event JSON:  {"shipment_id": 1, "origin": "Tokyo", "is_arrived": true}
Destination schema:   [shipment_id, order_id, origin, destination, is_arrived]

Reconciliation:
  - "destination" in destination schema but missing → SET NULL

Output JSON:          {"shipment_id": 1, "origin": "Tokyo", "is_arrived": true, "destination": null}
Side Output:          {table: "shipments", missing_cols: ["destination"]}
```

**CHANGE TYPE (`order_id` INT → BIGINT):**

```
Incoming event JSON:  {"shipment_id": 1, "order_id": 9999999999, "origin": "Tokyo"}
Destination schema:   [shipment_id (int), order_id (int), origin (varchar)]

Reconciliation:
  - All columns match by name → PASS THROUGH
  - Type mismatch detected at JDBC level (value too large for int) → runtime error

Note: type changes are harder to reconcile without a full type registry
```

### Design Trade-off

**You choose data availability over data completeness:**

```
Without reconciliation layer:
  Schema mismatch → INSERT fails → infinite retry → PIPELINE DOWN → no data flows

With reconciliation layer:
  Schema mismatch → unknown columns dropped, missing columns NULL → INSERT succeeds
  → PIPELINE STAYS UP → partial data flows + drift alert via side output
```

| Aspect | Without | With |
|--------|---------|------|
| Pipeline uptime | Breaks on any DDL mismatch | Stays running |
| Data completeness | All-or-nothing | May have NULLs for renamed/dropped columns |
| Visibility | Error in Flink logs | Structured side output for monitoring/alerting |
| Manual intervention | Required immediately (pipeline blocked) | Can be scheduled (pipeline running) |

### Implementation Approach

The reconciliation layer would be a `ProcessFunction` placed before the sink:

```
cdcStream
    .keyBy(CdcEvent::getTable)
    .process(new SchemaReconciliationFunction(destinationSchemaProvider))
    // Main output: reconciled CdcEvents (safe to sink)
    .sinkTo(pgSink);

// Side output: schema drift events (for alerting/monitoring)
reconciled.getSideOutput(schemaDriftTag)
    .sinkTo(alertSink);
```

Key decisions for implementation:

1. **Where to get destination schema** — query Postgres `information_schema` (like existing `getColumnTypes()`) with periodic refresh, or maintain a config map
2. **Side output format** — include table name, mismatched columns, event timestamp, drift type (unknown column, missing column, type mismatch)
3. **Cache TTL** — destination schema should refresh periodically (e.g., every 5 minutes) to pick up manual DDL changes on the Postgres side

## Recommended Mitigations (Progressive)

### Short-Term (No Code Changes)

- **ADD column:** Always DDL destination **before** source
- **DROP column:** Safe to drop source first; drop destination later if needed
- **RENAME / TYPE change:** Pause pipeline → DDL both databases → restart pipeline (clears `columnTypeCache`)

### Medium-Term (Small Code Changes)

- Add **TTL-based eviction** to `columnTypeCache` — even a 5-minute TTL catches most schema changes
- Add **try-catch with cache invalidation** around `ps.executeUpdate()` in `executeUpsert()` — on `PSQLException`, clear cache entry and retry once
- Implement the **schema reconciliation `ProcessFunction`** with side output for drift alerting

### Long-Term (Architectural)

- Intercept **Debezium DDL events** (emitted as `SourceRecord` with specific structure) to trigger automatic destination schema migration
- Integrate a **schema migration tool** (Flyway / Liquibase) to keep source and destination schemas in sync
- Build a **schema registry** that both source and destination consult for column mappings

## TL;DR

| Question | Answer |
|----------|--------|
| **What is schema evolution?** | Source schema changes (DDL) while the CDC pipeline is running |
| **What's the safest DDL?** | DROP column — pipeline adapts, destination gets NULLs |
| **What's the most dangerous DDL?** | RENAME column — no safe ordering, requires pipeline pause |
| **Why does the middle layer survive?** | Schema-driven iteration + opaque JSON + dynamic SQL |
| **Why does the edge break?** | Postgres schema is static + `columnTypeCache` never refreshes |
| **What's the mitigation?** | Schema reconciliation layer — normalize events against destination schema, side output drift alerts |
| **Trade-off?** | Data availability over data completeness — NULLs instead of crashes |

---

## Takeaways

### What You Should Learn From This Doc

1. **Schema evolution is the #1 operational risk in CDC pipelines** — databases change schemas regularly, and a pipeline that can't handle DDL changes will break in production
2. **The middle layers are resilient by design** — schema-driven `structToJson()`, opaque `CdcEvent`, and dynamic SQL in `PGSinker` all adapt automatically to schema changes. The fragility is at the edges (destination schema + type cache)
3. **Column rename is the hardest DDL to handle** — it's effectively a simultaneous DROP + ADD with no safe ordering. Every other DDL change has a safe migration path
4. **A schema reconciliation layer trades completeness for availability** — by normalizing events against the destination schema and side-outputting drift, the pipeline stays running with NULLs instead of crashing

### How This Helps You Understand the Flink Application

- You now know why `columnTypeCache` is a potential time bomb — it never expires, and any type change in MySQL can cause silent data corruption via stale cache entries
- You understand the architectural trade-off of the generic JSON approach — maximum flexibility for the common case, but no protection against schema drift at the edges
- You can evaluate whether the pipeline needs a schema reconciliation layer based on how frequently your source schemas change

### Other Benefits of This Knowledge

- **Design schema-aware CDC pipelines** — for any source/destination combination, you now know where schema knowledge lives and where it can diverge
- **Evaluate CDC tools** — products like Debezium Server, Striim, and Airbyte each handle schema evolution differently. This framework helps you evaluate their approaches
- **Plan DDL migrations** — you have a concrete runbook for each DDL type: which database to change first, whether to pause the pipeline, and what to monitor after
