# HikariCP - 5 Minute Quick Lesson

## What is HikariCP?

HikariCP (hikari = "light" in Japanese) is a **JDBC connection pool** library for Java. It is widely regarded as the **fastest and most lightweight** connection pool available. Spring Boot uses it as the **default connection pool** since Spring Boot 2.0.

## The Problem It Solves

Opening a database connection is **expensive**:

1. TCP handshake with the database server
2. Authentication (username/password verification)
3. Session initialization on the database side
4. Memory allocation on both client and server

If your app opens a new connection for every query and closes it after, you pay this cost **every single time**.

## Conventional (No Pool) vs HikariCP

### Without Connection Pool (Conventional)

```java
// Every request creates a new connection - SLOW
public User getUser(int id) throws SQLException {
    // 1. Open connection (~30-50ms each time!)
    Connection conn = DriverManager.getConnection(
        "jdbc:mysql://localhost:3306/db_1", "mysqluser", "mysqlpw"
    );

    // 2. Execute query
    PreparedStatement ps = conn.prepareStatement("SELECT * FROM users WHERE id = ?");
    ps.setInt(1, id);
    ResultSet rs = ps.executeQuery();

    // 3. Process result
    User user = mapToUser(rs);

    // 4. Close everything (connection is destroyed)
    rs.close();
    ps.close();
    conn.close();  // Connection gone forever, next call pays full cost again

    return user;
}
```

**Timeline per request:**
```
[--30ms connect--][--5ms query--][--close--]  = ~35ms
[--30ms connect--][--5ms query--][--close--]  = ~35ms
[--30ms connect--][--5ms query--][--close--]  = ~35ms
```

### With HikariCP

```java
// Setup once at application startup
HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:mysql://localhost:3306/db_1");
config.setUsername("mysqluser");
config.setPassword("mysqlpw");
config.setMaximumPoolSize(10);        // max 10 connections in the pool
config.setMinimumIdle(5);             // keep at least 5 ready
config.setConnectionTimeout(30000);   // wait max 30s for a connection
config.setIdleTimeout(600000);        // close idle connections after 10min
config.setMaxLifetime(1800000);       // recycle connections every 30min

// Create the pool (once)
HikariDataSource dataSource = new HikariDataSource(config);
```

```java
// Every request borrows a connection from the pool - FAST
public User getUser(int id) throws SQLException {
    // 1. Borrow from pool (~0.5ms, connection already open!)
    try (Connection conn = dataSource.getConnection()) {

        // 2. Execute query
        PreparedStatement ps = conn.prepareStatement("SELECT * FROM users WHERE id = ?");
        ps.setInt(1, id);
        ResultSet rs = ps.executeQuery();

        // 3. Process result
        return mapToUser(rs);

    }  // 4. Connection returned to pool (NOT closed), ready for next caller
}
```

**Timeline per request:**
```
[0.5ms borrow][--5ms query--][return]  = ~5.5ms
[0.5ms borrow][--5ms query--][return]  = ~5.5ms
[0.5ms borrow][--5ms query--][return]  = ~5.5ms
```

## Side-by-Side Comparison

| Aspect | No Pool (DriverManager) | HikariCP |
|---|---|---|
| **Connection cost** | ~30-50ms per request | ~0.5ms (borrow from pool) |
| **Max connections** | Unbounded (dangerous) | Configurable limit |
| **Connection reuse** | None, new every time | Connections are recycled |
| **Health checks** | None | Automatic (detects broken connections) |
| **Thread safety** | You manage it | Built-in |
| **Memory** | Spikes with load | Stable, bounded |
| **Under heavy load** | Can overwhelm DB | Queues requests, protects DB |

## Gradle Dependency

```groovy
// build.gradle
dependencies {
    implementation 'com.zaxxer:HikariCP:5.1.0'

    // You still need the JDBC driver for your database
    // Pick ONE (or more) depending on your database:
    implementation 'com.mysql:mysql-connector-j:8.2.0'       // MySQL
    implementation 'org.postgresql:postgresql:42.7.1'         // PostgreSQL
}
```

## Works With Any JDBC Database

HikariCP is **database-agnostic**. The only thing that changes is the JDBC URL and driver:

| Database   | JDBC URL                                         | Driver Dependency                          |
|------------|--------------------------------------------------|--------------------------------------------|
| MySQL      | `jdbc:mysql://localhost:3306/mydb`                | `com.mysql:mysql-connector-j:8.2.0`        |
| PostgreSQL | `jdbc:postgresql://localhost:5432/mydb`           | `org.postgresql:postgresql:42.7.1`          |
| SQL Server | `jdbc:sqlserver://localhost:1433;databaseName=mydb` | `com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11` |
| H2         | `jdbc:h2:mem:testdb`                              | `com.h2database:h2:2.2.224`                |

### PostgreSQL Example

```java
HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:postgresql://localhost:5432/mydb");
config.setUsername("postgres");
config.setPassword("postgres");
config.setMaximumPoolSize(10);

// PostgreSQL-specific tuning (optional)
config.addDataSourceProperty("cachePrepStmts", "true");
config.addDataSourceProperty("prepStmtCacheSize", "250");

HikariDataSource dataSource = new HikariDataSource(config);

// Usage is identical - HikariCP doesn't care which database
try (Connection conn = dataSource.getConnection()) {
    PreparedStatement ps = conn.prepareStatement("SELECT * FROM users WHERE id = $1");
    ps.setInt(1, userId);
    ResultSet rs = ps.executeQuery();
    // ...
}
```

## How the Pool Works (Visual)

```
Application Threads          HikariCP Pool             Database
                            ┌─────────────┐
Thread-1 ── getConnection() ─→│ conn-1 [busy] │──────── Session 1
Thread-2 ── getConnection() ─→│ conn-2 [busy] │──────── Session 2
Thread-3 ── getConnection() ─→│ conn-3 [busy] │──────── Session 3
Thread-4 ── getConnection() ─→│ conn-4 [idle] │──────── Session 4
                              │ conn-5 [idle] │──────── Session 5
Thread-5 ── waiting...        │               │
                              └─────────────┘
                              maxPoolSize = 5
                              (Thread-5 waits until
                               a connection is returned)
```

**Lifecycle:**
1. Pool pre-creates `minimumIdle` connections at startup
2. `getConnection()` borrows an idle connection (sub-millisecond)
3. `connection.close()` returns it to the pool (not actually closed)
4. If all connections are busy, callers wait up to `connectionTimeout`
5. Connections older than `maxLifetime` are recycled transparently

## Key Configuration Properties

```java
HikariConfig config = new HikariConfig();

// Required
config.setJdbcUrl("jdbc:mysql://localhost:3306/db_1");
config.setUsername("mysqluser");
config.setPassword("mysqlpw");

// Pool sizing (most important to tune)
config.setMaximumPoolSize(10);   // Rule of thumb: (2 * CPU cores) + disk spindles
config.setMinimumIdle(10);       // HikariCP recommends setting equal to max

// Timeouts
config.setConnectionTimeout(30000);  // 30s - max wait for a connection
config.setIdleTimeout(600000);       // 10min - how long idle connections live
config.setMaxLifetime(1800000);      // 30min - max age before recycling

// Validation
config.setConnectionTestQuery("SELECT 1");  // optional for modern drivers
```

## Common Usage in Flink Context

In a Flink application (like this project), HikariCP is useful for:

- **Async I/O enrichment** - when using `AsyncDataStream` to look up data from a database
- **Custom sinks** - writing results to a relational database
- **Custom sources** - reading from a database with controlled connection reuse

```java
// Example: Flink AsyncFunction with HikariCP
public class AsyncDatabaseLookup extends RichAsyncFunction<Event, EnrichedEvent> {
    private transient HikariDataSource dataSource;

    @Override
    public void open(Configuration parameters) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://localhost:3306/db_1");
        config.setUsername("mysqluser");
        config.setPassword("mysqlpw");
        config.setMaximumPoolSize(10);
        dataSource = new HikariDataSource(config);
    }

    @Override
    public void asyncInvoke(Event event, ResultFuture<EnrichedEvent> resultFuture) {
        CompletableFuture.supplyAsync(() -> {
            try (Connection conn = dataSource.getConnection()) {
                // fast borrow from pool, query, auto-return
                return queryDatabase(conn, event);
            }
        }).thenAccept(result -> resultFuture.complete(Collections.singleton(result)));
    }

    @Override
    public void close() {
        if (dataSource != null) dataSource.close();  // shut down pool
    }
}
```

## TL;DR

| Question | Answer |
|---|---|
| **What is it?** | A JDBC connection pool library |
| **Why use it?** | Reuses DB connections instead of opening/closing each time |
| **Performance gain?** | ~6-10x faster per query (eliminates connection overhead) |
| **Difficulty?** | Drop-in replacement, just swap `DriverManager` for `HikariDataSource` |
| **When to use?** | Any Java app that talks to a relational database |
