# java.sql.* — JDBC Database Operations Quick Lesson

## What Is JDBC?

JDBC (Java Database Connectivity) is Java's **built-in API** for talking to relational databases. It's part of the JDK — no external dependency needed for the API itself. You only need a **driver** for your specific database (e.g. `org.postgresql:postgresql` for Postgres).

The key classes all live in `java.sql.*`:

```java
import java.sql.Connection;          // a session with the database
import java.sql.PreparedStatement;   // a parameterized SQL query
import java.sql.ResultSet;           // rows returned from a SELECT
import java.sql.SQLException;        // the exception everything throws
```

## The Four Classes and How They Relate

```
DataSource / DriverManager
    │
    │  .getConnection()
    ▼
Connection                         ← a session/channel to the database
    │
    │  .prepareStatement(sql)
    ▼
PreparedStatement                  ← a compiled SQL statement with ? placeholders
    │
    │  .setObject(1, value)        ← fill in the ? placeholders
    │  .setString(2, "Tokyo")
    │
    ├── .executeUpdate()           ← for INSERT, UPDATE, DELETE → returns int (rows affected)
    │
    └── .executeQuery()            ← for SELECT → returns ResultSet
            │
            ▼
        ResultSet                  ← cursor over returned rows
            │
            │  .next()             ← move to next row (returns false when done)
            │  .getString("col")   ← read column value from current row
            │  .getInt("col")
            └──
```

## Learning by Example: Simple CRUD

### 1. Get a Connection

```java
// Option A: DriverManager (simple, no pooling — for learning)
Connection conn = DriverManager.getConnection(
    "jdbc:postgresql://localhost:5432/pgdb",   // JDBC URL
    "postgres",                                 // username
    "postgres"                                  // password
);

// Option B: HikariCP DataSource (pooled — for production, used in PGSinker)
HikariDataSource dataSource = new HikariDataSource(config);
Connection conn = dataSource.getConnection();   // borrows from pool
```

A `Connection` is like an open phone call to the database. While it's open, you can send SQL statements back and forth. When you're done, you close it (or return it to the pool).

### 2. INSERT a Row

```java
String sql = "INSERT INTO shipments (shipment_id, origin, is_arrived) VALUES (?, ?, ?)";
//                                                                           1  2  3
//                                                              placeholder indices ───┘

PreparedStatement ps = conn.prepareStatement(sql);
ps.setInt(1, 1);                    // ? #1 → 1
ps.setString(2, "Shanghai");        // ? #2 → 'Shanghai'
ps.setBoolean(3, false);            // ? #3 → false

int rowsAffected = ps.executeUpdate();  // → 1 (one row inserted)
```

### 3. SELECT Rows (Query)

```java
String sql = "SELECT shipment_id, origin, is_arrived FROM shipments WHERE origin = ?";

PreparedStatement ps = conn.prepareStatement(sql);
ps.setString(1, "Shanghai");

ResultSet rs = ps.executeQuery();

// ResultSet is a cursor — call .next() to move to each row
while (rs.next()) {
    int id       = rs.getInt("shipment_id");      // read by column name
    String origin = rs.getString("origin");
    boolean arrived = rs.getBoolean("is_arrived");

    System.out.println(id + " " + origin + " " + arrived);
}
// When rs.next() returns false, there are no more rows
```

**Visualizing the ResultSet cursor:**

```
ResultSet (after executeQuery)
  cursor → (before first row)

  rs.next() → true
  cursor → │ shipment_id=1 │ origin="Shanghai" │ is_arrived=false │   ← current row
           │ shipment_id=2 │ origin="Shanghai" │ is_arrived=true  │

  rs.next() → true
  cursor → │ shipment_id=2 │ origin="Shanghai" │ is_arrived=true  │   ← current row

  rs.next() → false   ← no more rows, loop ends
```

### 4. UPDATE a Row

```java
String sql = "UPDATE shipments SET is_arrived = ? WHERE shipment_id = ?";

PreparedStatement ps = conn.prepareStatement(sql);
ps.setBoolean(1, true);   // SET is_arrived = true
ps.setInt(2, 1);           // WHERE shipment_id = 1

int rowsAffected = ps.executeUpdate();  // → 1 (one row updated)
```

### 5. DELETE a Row

```java
String sql = "DELETE FROM shipments WHERE shipment_id = ?";

PreparedStatement ps = conn.prepareStatement(sql);
ps.setInt(1, 1);

int rowsAffected = ps.executeUpdate();  // → 1 (one row deleted)
```

### 6. UPSERT (INSERT ... ON CONFLICT)

Postgres-specific syntax. Inserts if the row doesn't exist, updates if it does:

```java
String sql = """
    INSERT INTO shipments (shipment_id, origin, is_arrived)
    VALUES (?, ?, ?)
    ON CONFLICT (shipment_id)
    DO UPDATE SET origin = EXCLUDED.origin, is_arrived = EXCLUDED.is_arrived
    """;

PreparedStatement ps = conn.prepareStatement(sql);
ps.setInt(1, 1);
ps.setString(2, "Tokyo");
ps.setBoolean(3, true);

ps.executeUpdate();
// If shipment_id=1 exists    → UPDATE origin='Tokyo', is_arrived=true
// If shipment_id=1 not exist → INSERT (1, 'Tokyo', true)
```

`EXCLUDED` refers to the row that was proposed for insertion. `EXCLUDED.origin` means "the origin value from the VALUES clause."

## executeUpdate vs executeQuery

These are the two ways to run a `PreparedStatement`:

| Method | Use For | Returns |
|--------|---------|---------|
| `executeUpdate()` | INSERT, UPDATE, DELETE, CREATE TABLE | `int` — number of rows affected |
| `executeQuery()` | SELECT | `ResultSet` — rows of data |

```java
// Writing data → executeUpdate()
ps.executeUpdate();    // → 1 (rows affected)

// Reading data → executeQuery()
ResultSet rs = ps.executeQuery();
while (rs.next()) { ... }
```

If you call the wrong one, you get a `SQLException`.

## PreparedStatement: Why ? Placeholders Matter

### The SQL Injection Problem

```java
// DANGEROUS: string concatenation
String name = userInput;  // imagine user types: "'; DROP TABLE shipments; --"
String sql = "SELECT * FROM shipments WHERE origin = '" + name + "'";
// Becomes: SELECT * FROM shipments WHERE origin = ''; DROP TABLE shipments; --'
// → your table is gone
```

### The Solution: Parameterized Queries

```java
// SAFE: PreparedStatement with ?
String sql = "SELECT * FROM shipments WHERE origin = ?";
PreparedStatement ps = conn.prepareStatement(sql);
ps.setString(1, "'; DROP TABLE shipments; --");
// The database treats the ENTIRE string as a value, not as SQL
// → no injection, just searches for a weird origin name
```

**Rule:** NEVER concatenate user/external values into SQL strings. Always use `?` placeholders.

### Placeholder Indices Start at 1 (Not 0!)

```java
String sql = "INSERT INTO t (a, b, c) VALUES (?, ?, ?)";
//                                             1  2  3

ps.setInt(1, 100);       // first ?
ps.setString(2, "hello"); // second ?
ps.setBoolean(3, true);  // third ?
```

This is a common source of bugs if you're used to zero-indexed arrays. JDBC uses **1-based indexing** for all parameter positions.

### setObject vs Typed Setters

```java
// Typed setters — you specify the Java type
ps.setInt(1, 42);
ps.setString(2, "Shanghai");
ps.setBoolean(3, true);
ps.setLong(4, 1700000000000L);
ps.setDouble(5, 29.99);
ps.setNull(6, java.sql.Types.VARCHAR);

// Generic setter — JDBC driver figures out the SQL type
ps.setObject(1, 42);           // driver infers INTEGER
ps.setObject(2, "Shanghai");   // driver infers VARCHAR
ps.setObject(3, true);         // driver infers BOOLEAN
ps.setObject(4, null);         // driver infers NULL
```

`setObject()` is more flexible — the JDBC driver auto-maps Java types to SQL types. This is what PGSinker uses because the column types are dynamic (we don't know at compile time if a column is INT, VARCHAR, or BOOLEAN).

```java
// PGSinker uses setObject because columns are discovered at runtime
for (int i = 0; i < columns.size(); i++) {
    ps.setObject(i + 1, convertValue(values.get(i), pgType));
    //           ^^^^^ 1-based index
}
```

## ResultSet: Reading Query Results

`ResultSet` is a **cursor** over the rows returned by a SELECT. You advance it row by row with `.next()`:

```java
ResultSet rs = ps.executeQuery();

while (rs.next()) {
    // Read columns from the CURRENT row
    String name   = rs.getString("column_name");    // by column name
    String type   = rs.getString("data_type");       // by column name
    int id        = rs.getInt(1);                    // by column index (1-based)
}
```

**ResultSet access methods** (similar to Struct!):

| Method | Returns | SQL Types |
|--------|---------|-----------|
| `getString("col")` | `String` | VARCHAR, TEXT, CHAR |
| `getInt("col")` | `int` | INTEGER, SMALLINT |
| `getLong("col")` | `long` | BIGINT |
| `getDouble("col")` | `double` | DOUBLE, DECIMAL |
| `getBoolean("col")` | `boolean` | BOOLEAN |
| `getObject("col")` | `Object` | any (generic) |

### How PGSinker Uses ResultSet

The `getColumnTypes()` method queries Postgres metadata and reads the result:

```java
String sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?";

PreparedStatement ps = conn.prepareStatement(sql);
ps.setString(1, "shipments");

ResultSet rs = ps.executeQuery();
// Returns rows like:
//   column_name  | data_type
//   -------------|----------
//   shipment_id  | integer
//   origin       | character varying
//   is_arrived   | boolean

while (rs.next()) {
    String colName = rs.getString("column_name");   // "shipment_id"
    String colType = rs.getString("data_type");     // "integer"
    types.put(colName, colType);
}
// types = {"shipment_id": "integer", "origin": "character varying", "is_arrived": "boolean"}
```

## Connection: Transactions and AutoCommit

### AutoCommit Mode (Default)

By default, every SQL statement is its own transaction — it's committed immediately:

```java
Connection conn = dataSource.getConnection();
// autoCommit = true (default)

ps1.executeUpdate();  // → committed immediately
ps2.executeUpdate();  // → committed immediately
// If ps2 fails, ps1 is already committed — can't undo it
```

### Manual Transaction (setAutoCommit(false))

For batch operations, you want all-or-nothing:

```java
Connection conn = dataSource.getConnection();
conn.setAutoCommit(false);           // BEGIN transaction

try {
    ps1.executeUpdate();              // not committed yet
    ps2.executeUpdate();              // not committed yet
    ps3.executeUpdate();              // not committed yet

    conn.commit();                    // COMMIT — all three become visible at once
} catch (Exception e) {
    conn.rollback();                  // ROLLBACK — undo all three, as if nothing happened
    throw e;
}
```

**Visualizing the difference:**

```
AutoCommit ON (default):

  Statement 1 ──► [execute + commit] ✓ visible in DB
  Statement 2 ──► [execute + commit] ✓ visible in DB
  Statement 3 ──► [execute] ✗ FAILS
                   → Statement 1 and 2 already committed, can't undo!

AutoCommit OFF (manual transaction):

  BEGIN
  Statement 1 ──► [execute] (pending, not visible to other connections)
  Statement 2 ──► [execute] (pending)
  Statement 3 ──► [execute] ✗ FAILS
  ROLLBACK        → ALL three undone, DB unchanged

  — or if all succeed —

  BEGIN
  Statement 1 ──► [execute] (pending)
  Statement 2 ──► [execute] (pending)
  Statement 3 ──► [execute] (pending)
  COMMIT          → ALL three visible at once, atomically
```

### How PGSinker Uses Transactions

```java
// In flush() — one transaction for the entire batch
try (Connection conn = dataSource.getConnection()) {
    conn.setAutoCommit(false);                 // BEGIN

    try {
        for (CdcEvent event : buffer) {
            // each executeUpsert/executeDelete runs on this SAME connection
            // within the SAME transaction
            executeUpsert(conn, table, pks, json);
        }
        conn.commit();                         // all succeed → COMMIT
        buffer.clear();
    } catch (Exception e) {
        conn.rollback();                       // any failure → ROLLBACK everything
        throw e;
    }
}
```

Why pass `conn` to `executeUpsert`/`executeDelete`? Because all statements must run on the **same connection** to be in the **same transaction**. If each method got its own connection from the pool, they'd be in separate transactions.

## try-with-resources: Auto-Closing

JDBC resources (`Connection`, `PreparedStatement`, `ResultSet`) must be closed after use. Forgetting to close them causes **resource leaks** (connection pool exhaustion, memory leaks).

### Without try-with-resources (verbose, error-prone)

```java
Connection conn = null;
PreparedStatement ps = null;
ResultSet rs = null;

try {
    conn = dataSource.getConnection();
    ps = conn.prepareStatement("SELECT * FROM shipments");
    rs = ps.executeQuery();

    while (rs.next()) { ... }
} finally {
    // Must close in reverse order, and each can throw
    if (rs != null) try { rs.close(); } catch (SQLException e) { }
    if (ps != null) try { ps.close(); } catch (SQLException e) { }
    if (conn != null) try { conn.close(); } catch (SQLException e) { }
}
```

### With try-with-resources (clean, safe)

```java
try (Connection conn = dataSource.getConnection();
     PreparedStatement ps = conn.prepareStatement("SELECT * FROM shipments");
     ResultSet rs = ps.executeQuery()) {

    while (rs.next()) { ... }

}  // conn, ps, rs all auto-closed here, even if an exception is thrown
```

**How it works:** Any object that implements `AutoCloseable` (which `Connection`, `PreparedStatement`, and `ResultSet` all do) can go in the `try (...)` parentheses. Java guarantees `.close()` is called when the block exits — whether normally or via exception.

### Nested try-with-resources in PGSinker

PGSinker uses nested try-with-resources because the `Connection` and `PreparedStatement` have different lifetimes:

```java
// Outer: Connection lives for the entire batch
try (Connection conn = dataSource.getConnection()) {
    conn.setAutoCommit(false);

    // ... loop over events ...

        // Inner: PreparedStatement lives for one SQL execution
        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            ps.setObject(1, value);
            ps.executeUpdate();
        }  // ps closed here, but conn stays open

    // ... more events ...

    conn.commit();
}  // conn closed (returned to pool) here
```

## Building Dynamic SQL

PGSinker builds SQL strings dynamically because it doesn't know table/column names at compile time. Here's how `executeUpsert` constructs the SQL step by step:

```java
// Given: table = "shipments", columns = ["shipment_id", "origin", "is_arrived"]
//        pks = ["shipment_id"]

StringBuilder sql = new StringBuilder();

// Step 1: INSERT INTO "shipments" ("shipment_id", "origin", "is_arrived")
sql.append("INSERT INTO ").append(table).append(" (");
sql.append(String.join(", ", quotedColumns));       // "shipment_id", "origin", "is_arrived"
sql.append(") VALUES (");
sql.append(String.join(", ", columns.stream().map(c -> "?").toList()));  // ?, ?, ?
sql.append(")");
// sql = INSERT INTO "shipments" ("shipment_id", "origin", "is_arrived") VALUES (?, ?, ?)

// Step 2: ON CONFLICT ("shipment_id") DO UPDATE SET
sql.append(" ON CONFLICT (");
sql.append(String.join(", ", quotedPks));            // "shipment_id"
sql.append(") DO UPDATE SET ");

// Step 3: "origin" = EXCLUDED."origin", "is_arrived" = EXCLUDED."is_arrived"
List<String> updateClauses = columns.stream()
    .filter(c -> !pks.contains(c))                   // skip PK columns (can't update PK)
    .map(c -> quoteIdentifier(c) + " = EXCLUDED." + quoteIdentifier(c))
    .toList();
sql.append(String.join(", ", updateClauses));

// Final SQL:
// INSERT INTO "shipments" ("shipment_id", "origin", "is_arrived")
// VALUES (?, ?, ?)
// ON CONFLICT ("shipment_id")
// DO UPDATE SET "origin" = EXCLUDED."origin", "is_arrived" = EXCLUDED."is_arrived"
```

Then the `?` placeholders are filled:

```java
try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
    // columns  = ["shipment_id", "origin", "is_arrived"]
    // values   = [1,             "Shanghai", 0          ]
    // colTypes = {"shipment_id": "integer", "origin": "character varying", "is_arrived": "boolean"}

    for (int i = 0; i < columns.size(); i++) {
        String pgType = colTypes.get(columns.get(i));     // look up Postgres type
        Object value = convertValue(values.get(i), pgType); // fix type mismatches
        ps.setObject(i + 1, value);                        // 1-based index!
    }
    // ps is now: INSERT INTO "shipments" (...) VALUES (1, 'Shanghai', false) ON CONFLICT ...

    ps.executeUpdate();
}
```

## quoteIdentifier: Why Double-Quote Table/Column Names

In SQL, **identifiers** (table names, column names) and **values** are treated differently:

```sql
-- Values use single quotes
INSERT INTO shipments (origin) VALUES ('Shanghai');

-- Identifiers use double quotes (optional, but prevents issues)
INSERT INTO "shipments" ("origin") VALUES ('Shanghai');
```

**Why quote identifiers?**

1. **Reserved words:** A column named `order` or `table` would break without quotes:
   ```sql
   SELECT * FROM order          -- syntax error! "order" is a SQL keyword
   SELECT * FROM "order"        -- works
   ```

2. **Case sensitivity:** Unquoted identifiers are lowercased by Postgres. If your column is `userId`, you need `"userId"` to preserve the case.

3. **SQL injection on identifiers:** The `?` placeholder only works for values, not identifiers:
   ```java
   // Can't do this — ? doesn't work for table names
   conn.prepareStatement("SELECT * FROM ? WHERE ? = ?");

   // Must build the identifier into the SQL string — so quote it
   conn.prepareStatement("SELECT * FROM \"shipments\" WHERE \"id\" = ?");
   ```

The `quoteIdentifier()` method in PGSinker handles this:

```java
private String quoteIdentifier(String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
}

quoteIdentifier("shipments")   // → "\"shipments\""    (in SQL: "shipments")
quoteIdentifier("user\"name")  // → "\"user\"\"name\"" (in SQL: "user""name") — escaped
```

The `.replace("\"", "\"\"")` escapes any double quotes inside the identifier by doubling them — the SQL standard way to include a literal `"` in a quoted identifier.

## Mapping PGSinker Code to JDBC Concepts

Here's every `java.sql.*` usage in PGSinker mapped to what you learned:

| PGSinker Code | JDBC Concept | Section Above |
|---------------|-------------|---------------|
| `dataSource.getConnection()` | Get a connection (from pool) | Get a Connection |
| `conn.setAutoCommit(false)` | Begin a transaction | Transactions |
| `conn.commit()` | Commit all pending writes | Transactions |
| `conn.rollback()` | Undo all pending writes | Transactions |
| `conn.prepareStatement(sql)` | Compile SQL with ? placeholders | PreparedStatement |
| `ps.setObject(i+1, value)` | Fill in a ? placeholder (1-based) | setObject vs Typed Setters |
| `ps.setString(1, tableName)` | Fill in a string ? placeholder | setObject vs Typed Setters |
| `ps.executeUpdate()` | Run INSERT/UPDATE/DELETE | executeUpdate vs executeQuery |
| `ps.executeQuery()` | Run SELECT, get ResultSet | executeUpdate vs executeQuery |
| `rs.next()` | Move cursor to next row | ResultSet |
| `rs.getString("column_name")` | Read a string column from current row | ResultSet |
| `try (Connection conn = ...)` | Auto-close when block exits | try-with-resources |
| `try (PreparedStatement ps = ...)` | Auto-close when block exits | try-with-resources |
| `try (ResultSet rs = ...)` | Auto-close when block exits | try-with-resources |

## Quick Reference

```java
// ── Get connection ──
Connection conn = dataSource.getConnection();

// ── Write data (INSERT / UPDATE / DELETE) ──
PreparedStatement ps = conn.prepareStatement("INSERT INTO t (a, b) VALUES (?, ?)");
ps.setObject(1, value1);        // 1-based!
ps.setObject(2, value2);
int rows = ps.executeUpdate();  // returns rows affected

// ── Read data (SELECT) ──
PreparedStatement ps = conn.prepareStatement("SELECT a, b FROM t WHERE a = ?");
ps.setString(1, "filter");
ResultSet rs = ps.executeQuery();
while (rs.next()) {
    rs.getString("a");
    rs.getInt("b");
}

// ── Transactions ──
conn.setAutoCommit(false);       // BEGIN
try {
    // ... multiple statements ...
    conn.commit();               // all succeed
} catch (Exception e) {
    conn.rollback();             // all fail
}

// ── Auto-close resources ──
try (Connection conn = dataSource.getConnection();
     PreparedStatement ps = conn.prepareStatement(sql)) {
    ps.executeUpdate();
}  // both closed automatically
```

## TL;DR

| Question | Answer |
|----------|--------|
| **What is JDBC?** | Java's built-in API for talking to relational databases (`java.sql.*`) |
| **Connection** | An open session to the database — get from DataSource or DriverManager |
| **PreparedStatement** | A parameterized SQL query — use `?` placeholders, NEVER string concat |
| **ResultSet** | A cursor over SELECT results — advance with `.next()`, read with `.getString()` etc. |
| **executeUpdate()** | For INSERT/UPDATE/DELETE — returns rows affected |
| **executeQuery()** | For SELECT — returns ResultSet |
| **? index** | 1-based, not 0-based! `ps.setObject(1, ...)` is the first placeholder |
| **setObject vs setInt** | `setObject` is generic (driver infers type); `setInt` is explicit. PGSinker uses `setObject` for dynamic columns |
| **Transactions** | `setAutoCommit(false)` → multiple statements → `commit()` or `rollback()` |
| **try-with-resources** | `try (Connection c = ...) { }` auto-closes even on exception |
| **Why quote identifiers** | Prevents SQL injection on table/column names and handles reserved words |

---

## Takeaways

### What You Should Learn From This Doc

1. **Three core classes: Connection, PreparedStatement, ResultSet** — Connection opens a session, PreparedStatement sends parameterized SQL, ResultSet reads results. This is the foundation of all JDBC work
2. **Always use `?` placeholders, never string concatenation** — `PreparedStatement` with `?` prevents SQL injection. This is non-negotiable in production code
3. **Transactions = setAutoCommit(false) + commit/rollback** — without explicit transactions, each statement auto-commits. Wrapping multiple statements in a transaction makes them atomic (all-or-nothing)
4. **try-with-resources guarantees cleanup** — `Connection`, `PreparedStatement`, and `ResultSet` all implement `AutoCloseable`. Using `try (var x = ...)` ensures they're closed even on exceptions
5. **JDBC indexes are 1-based** — `ps.setObject(1, ...)` is the first placeholder, not `0`. This trips up every Java developer at least once

### How This Helps You Understand the Flink Application

- You can now read `PGSinker.executeUpsert()` and follow the dynamic SQL construction: building column lists, `?` placeholders, ON CONFLICT clause, then setting values with `ps.setObject()`
- You understand why `flush()` uses `setAutoCommit(false)` + `commit()` — it's a batch transaction wrapping all CDC events for atomicity
- You see why `getColumnTypes()` uses its own `try (Connection conn = ...)` — separate connection for metadata reads, not part of the write transaction

### Other Benefits of This Knowledge

- **Read any Java database code** — Spring JdbcTemplate, MyBatis, Hibernate all build on top of `java.sql`. Understanding raw JDBC means you can debug any ORM
- **Build dynamic SQL generators** — the pattern of iterating columns and building SQL strings with `StringBuilder` is used in query builders, migration tools, and schema generators
- **Work with any JDBC database** — Postgres, MySQL, Snowflake, Oracle, SQLite all speak JDBC. The same Connection/PreparedStatement/ResultSet pattern works for all of them
- **Understand connection pool behavior** — HikariCP wraps `java.sql.Connection`. Knowing what `close()` does on a raw connection vs a pooled one prevents resource leaks
