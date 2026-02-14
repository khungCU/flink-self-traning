# Schema Conversion Tools (AWS SCT & SnowConvert) - Quick Lesson

## The Problem

You have 50 tables in MySQL and need the same schema in Postgres (or Snowflake). Manually rewriting `CREATE TABLE` statements means:

- Translating type differences (`TINYINT(1)` → `BOOLEAN`, `AUTO_INCREMENT` → `SERIAL`, `DATETIME` → `TIMESTAMP`)
- Converting indexes, constraints, foreign keys
- Handling MySQL-specific syntax that doesn't exist in the target (`ENGINE=InnoDB`, `UNSIGNED`, etc.)
- Doing this for every table, and again whenever the source schema changes

Schema conversion tools automate this — feed in the source DDL, get target DDL out.

```
MySQL DDL ──► Schema Conversion Tool ──► Postgres / Snowflake DDL
              (type mapping,
               syntax rewrite,
               compatibility report)
```

## DDL vs DML Reminder

| | DDL (Data Definition Language) | DML (Data Manipulation Language) |
|---|---|---|
| **What** | Defines structure | Manipulates data |
| **Commands** | CREATE, ALTER, DROP, TRUNCATE | INSERT, UPDATE, DELETE, SELECT |
| **Example** | `CREATE TABLE shipments (...)` | `INSERT INTO shipments VALUES (...)` |
| **Schema tools handle** | This part | Not this part (your Flink CDC pipeline handles DML) |

## Tool Comparison

| | AWS SCT | SnowConvert | pgLoader | DMS |
|---|---|---|---|---|
| **Vendor** | AWS | Snowflake (Mobilize.Net) | Open source | AWS |
| **What it converts** | DDL + stored procedures + views | DDL + stored procedures + views | DDL + data (full migration) | DDL + ongoing data replication |
| **Target databases** | Postgres, Aurora, Redshift, Snowflake, and more | Snowflake only | Postgres only | Aurora, Redshift, Postgres, and more |
| **Source databases** | MySQL, Oracle, SQL Server, Teradata, and more | MySQL, Oracle, SQL Server, Teradata, Redshift, and more | MySQL, SQLite, MS SQL | MySQL, Oracle, SQL Server, and more |
| **Cost** | Free (part of AWS) | Free tier available, paid for enterprise | Free | Pay per usage |
| **Interface** | Desktop GUI (Java app) | Web-based + CLI | CLI | AWS Console |
| **Best for** | AWS-centric teams, multi-target migrations | Snowflake migrations specifically | Quick MySQL → Postgres migrations | Full migration with ongoing replication |

## AWS SCT (Schema Conversion Tool)

### What It Is

A free desktop application from AWS that converts database schemas between different engines. It analyzes your source schema, generates equivalent DDL for the target, and produces an **assessment report** highlighting what can be auto-converted and what needs manual intervention.

### Quick Start

#### 1. Download and Install

```
Download from: https://docs.aws.amazon.com/SchemaConversionTool/latest/userguide/CHAP_Installing.html
```

It's a Java desktop application — works on Windows, macOS, Linux. Requires JDK 8+.

#### 2. Create a New Project

1. Open AWS SCT
2. **File → New Project**
3. Give it a name (e.g. "mysql-to-postgres-migration")

#### 3. Connect to Source (MySQL)

1. **Add source** → select **MySQL**
2. Fill in connection details:
   ```
   Server name:  localhost
   Port:         3306
   User name:    mysqluser
   Password:     mysqlpw
   Database:     db_1
   ```
3. **Test connection** → **OK**

#### 4. Connect to Target (Postgres)

1. **Add target** → select **PostgreSQL**
2. Fill in connection details:
   ```
   Server name:  localhost
   Port:         5432
   User name:    postgres
   Password:     postgres
   Database:     pgdb
   ```
3. **Test connection** → **OK**

#### 5. Convert Schema

1. In the left panel (source), expand the database tree
2. Right-click the schema (e.g. `db_1`) → **Convert schema**
3. SCT generates the equivalent Postgres DDL in the right panel

#### 6. Review the Assessment Report

SCT produces a **migration assessment report** showing:

```
┌─────────────────────────────────────────────────────┐
│ Assessment Report: db_1 → pgdb                      │
│                                                     │
│ Total objects:        52                            │
│ Auto-converted:       48 (92%)   ← no manual work  │
│ With warnings:         3 (6%)    ← review needed    │
│ Cannot convert:        1 (2%)    ← manual rewrite   │
│                                                     │
│ Issues:                                             │
│ ⚠ Table "orders": UNSIGNED INT → INT (no unsigned   │
│   in Postgres, values will still fit)               │
│ ⚠ View "v_summary": MySQL-specific function         │
│   GROUP_CONCAT → must rewrite as STRING_AGG         │
│ ✗ Stored proc "sp_audit": uses MySQL-specific       │
│   HANDLER syntax → manual rewrite required          │
└─────────────────────────────────────────────────────┘
```

The color coding:
- **Green** — auto-converted, no issues
- **Yellow** — converted with warnings, review recommended
- **Red** — cannot auto-convert, manual intervention needed

#### 7. Apply to Target

1. In the right panel (target), right-click the converted schema
2. **Apply to database** → executes the DDL on your Postgres instance
3. Tables, indexes, constraints are created automatically

#### What It Converts (MySQL → Postgres Example)

```sql
-- MySQL source DDL
CREATE TABLE shipments (
    shipment_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    origin VARCHAR(255),
    destination VARCHAR(255),
    is_arrived TINYINT(1) DEFAULT 0,
    weight DOUBLE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    notes TEXT,
    INDEX idx_order (order_id)
) ENGINE=InnoDB;

-- AWS SCT converts to Postgres DDL
CREATE TABLE shipments (
    shipment_id SERIAL PRIMARY KEY,           -- AUTO_INCREMENT → SERIAL
    order_id INT NOT NULL,
    origin VARCHAR(255),
    destination VARCHAR(255),
    is_arrived BOOLEAN DEFAULT FALSE,          -- TINYINT(1) → BOOLEAN
    weight DOUBLE PRECISION,                   -- DOUBLE → DOUBLE PRECISION
    created_at TIMESTAMP DEFAULT NOW(),        -- DATETIME → TIMESTAMP
    notes TEXT
);
CREATE INDEX idx_order ON shipments (order_id); -- INDEX moved outside CREATE TABLE
-- ENGINE=InnoDB removed (Postgres doesn't have storage engines)
```

### Common MySQL → Postgres Type Mappings (SCT)

| MySQL | Postgres | Notes |
|-------|----------|-------|
| `INT` | `INTEGER` | Same |
| `INT AUTO_INCREMENT` | `SERIAL` | Auto-incrementing |
| `BIGINT AUTO_INCREMENT` | `BIGSERIAL` | Auto-incrementing 64-bit |
| `TINYINT(1)` | `BOOLEAN` | MySQL's boolean workaround |
| `TINYINT` | `SMALLINT` | When not used as boolean |
| `DOUBLE` | `DOUBLE PRECISION` | Same precision |
| `FLOAT` | `REAL` | Single precision |
| `DATETIME` | `TIMESTAMP` | Without timezone |
| `TIMESTAMP` | `TIMESTAMP WITH TIME ZONE` | With timezone |
| `VARCHAR(N)` | `VARCHAR(N)` | Same |
| `TEXT` / `LONGTEXT` | `TEXT` | Postgres TEXT has no length limit |
| `BLOB` / `LONGBLOB` | `BYTEA` | Binary data |
| `ENUM('a','b','c')` | `VARCHAR` + CHECK constraint | Postgres has native ENUM too, but CHECK is more portable |
| `JSON` | `JSONB` | Postgres binary JSON (more efficient) |
| `UNSIGNED INT` | `INTEGER` + CHECK constraint | Postgres has no unsigned types |

### AWS SCT Tips

- **You don't need an AWS account** to use SCT for local conversions — it's a free desktop tool
- **Save the project** — you can re-run conversion when source schema changes
- **Export DDL as SQL file** — right-click converted schema → Save as SQL for version control
- SCT also converts **views, stored procedures, functions, and triggers** (with varying success depending on complexity)

---

## SnowConvert

### What It Is

Snowflake's official schema and code conversion tool. Originally built by Mobilize.Net, acquired by Snowflake. Converts DDL, DML, stored procedures, and SQL scripts from various sources into Snowflake-compatible SQL.

### Quick Start

#### Option A: Web Interface (Easiest)

1. Go to **https://www.snowconvert.com**
2. Sign up for a free account
3. **Upload your SQL file** (e.g. your MySQL `CREATE TABLE` statements)
4. Select source dialect: **MySQL**
5. Click **Convert**
6. Download the converted Snowflake DDL

#### Option B: CLI Tool

```bash
# Install (requires Node.js)
npm install -g @snowflake/snowconvert

# Convert a single SQL file
snowconvert --source mysql --input ./mysql-schema.sql --output ./snowflake-schema.sql

# Convert an entire directory
snowconvert --source mysql --input ./mysql-ddl/ --output ./snowflake-ddl/
```

#### Option C: Inside Snowsight (Snowflake Web UI)

1. Log into Snowflake → open **Snowsight**
2. Navigate to **Data → Migration**
3. Upload your source DDL files
4. SnowConvert runs automatically within Snowflake's UI

#### What It Converts (MySQL → Snowflake Example)

```sql
-- MySQL source DDL
CREATE TABLE shipments (
    shipment_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    origin VARCHAR(255),
    destination VARCHAR(255),
    is_arrived TINYINT(1) DEFAULT 0,
    weight DOUBLE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    notes LONGTEXT,
    INDEX idx_order (order_id)
) ENGINE=InnoDB;

-- SnowConvert converts to Snowflake DDL
CREATE TABLE shipments (
    shipment_id INT AUTOINCREMENT PRIMARY KEY,   -- AUTO_INCREMENT → AUTOINCREMENT
    order_id INT NOT NULL,
    origin VARCHAR(255),
    destination VARCHAR(255),
    is_arrived BOOLEAN DEFAULT FALSE,             -- TINYINT(1) → BOOLEAN
    weight FLOAT,                                 -- DOUBLE → FLOAT (64-bit in Snowflake)
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), -- DATETIME → TIMESTAMP_NTZ
    notes VARCHAR(16777216)                       -- LONGTEXT → VARCHAR(max)
    -- INDEX removed (Snowflake auto-optimizes via micro-partitions, no manual indexes)
    -- ENGINE=InnoDB removed (not applicable)
);
```

### Common MySQL → Snowflake Type Mappings (SnowConvert)

| MySQL | Snowflake | Notes |
|-------|-----------|-------|
| `INT` | `NUMBER(38,0)` or `INT` | Snowflake INT is alias for NUMBER(38,0) |
| `AUTO_INCREMENT` | `AUTOINCREMENT` or `IDENTITY` | Slightly different keyword |
| `BIGINT` | `NUMBER(38,0)` | Snowflake uses NUMBER for all integers |
| `TINYINT(1)` | `BOOLEAN` | |
| `DOUBLE` / `FLOAT` | `FLOAT` | Snowflake FLOAT is always 64-bit |
| `DECIMAL(10,2)` | `NUMBER(10,2)` | |
| `DATETIME` | `TIMESTAMP_NTZ` | No timezone |
| `TIMESTAMP` | `TIMESTAMP_LTZ` | Local timezone |
| `VARCHAR(255)` | `VARCHAR(255)` | Same (max 16MB in Snowflake) |
| `TEXT` / `LONGTEXT` | `VARCHAR(16777216)` | Snowflake has no TEXT type, uses max VARCHAR |
| `BLOB` | `BINARY` | |
| `JSON` | `VARIANT` | Snowflake's semi-structured type |
| `ENUM(...)` | `VARCHAR` + comment | No native ENUM |

### Key Snowflake Differences to Know

```
┌─────────────────────────────────────────────────────────────┐
│ Snowflake is NOT Postgres                                   │
│                                                             │
│ • No indexes         — Snowflake auto-optimizes with        │
│                        micro-partitions and pruning          │
│ • No PRIMARY KEY     — it's accepted in DDL but NOT         │
│   enforcement          enforced (informational only!)        │
│ • No ON CONFLICT     — use MERGE INTO for upserts           │
│ • No SERIAL          — use AUTOINCREMENT or sequences       │
│ • No foreign key     — accepted but NOT enforced            │
│   enforcement                                               │
│ • No UPDATE with     — Snowflake is columnar/analytical,    │
│   single-row speed     not designed for row-level OLTP      │
└─────────────────────────────────────────────────────────────┘
```

This means if you ever point your Flink CDC pipeline at Snowflake instead of Postgres, the `PGSinker` would need significant changes:
- Replace `INSERT ... ON CONFLICT` with `MERGE INTO`
- Or better: stage CDC events to S3/GCS files, then use **Snowpipe** for ingestion

---

## Workflow: Schema Conversion + Flink CDC

Here's how schema conversion tools fit into your MySQL-to-Postgres mirroring workflow:

```
One-time setup:
                                    AWS SCT / SnowConvert
MySQL source schema ──────────────────────────────────────► Postgres/Snowflake target schema
  CREATE TABLE shipments (...)                                CREATE TABLE shipments (...)
  CREATE TABLE orders (...)                                   CREATE TABLE orders (...)
  CREATE TABLE users (...)                                    CREATE TABLE users (...)

Ongoing replication:
MySQL data changes ──► Flink CDC Pipeline ──► PGSinker ──► Postgres (tables already exist)
  INSERT/UPDATE/DELETE    (JsonCdcDeserializer)                 (upsert/delete via JDBC)
```

**Step 1 (once):** Use SCT/SnowConvert to create the target tables.
**Step 2 (ongoing):** Your Flink CDC pipeline replicates the data changes.

### Handling Schema Changes Over Time

When the MySQL source schema changes (e.g. `ALTER TABLE shipments ADD COLUMN weight DOUBLE`):

```
Option A: Manual
  1. Run ALTER TABLE on Postgres manually
  2. Restart Flink CDC pipeline

Option B: Re-run SCT
  1. Re-run SCT on the updated MySQL schema
  2. SCT generates the ALTER TABLE for Postgres (or a diff)
  3. Apply to Postgres
  4. Restart Flink CDC pipeline

Option C: AWS DMS (fully automated)
  DMS handles both schema changes AND data replication
  (but then you don't need your Flink pipeline at all)
```

---

## Quick Start: Extracting Your MySQL DDL

Before using any conversion tool, you need to export your MySQL schema:

```bash
# Export ALL table DDL from db_1 (schema only, no data)
docker exec flink-self-traning-mysql-1 mysqldump -uroot -p123456 \
    --no-data --skip-comments db_1 > mysql-schema.sql

# Export specific tables only
docker exec flink-self-traning-mysql-1 mysqldump -uroot -p123456 \
    --no-data --skip-comments db_1 shipments shipments_v0 > mysql-schema.sql
```

The output `mysql-schema.sql` is what you feed into SCT or SnowConvert.

```bash
# Preview what was exported
cat mysql-schema.sql
```

Example output:
```sql
CREATE TABLE `shipments` (
  `shipment_id` int(11) NOT NULL,
  `order_id` int(11) DEFAULT NULL,
  `origin` varchar(255) DEFAULT NULL,
  `destination` varchar(255) DEFAULT NULL,
  `is_arrived` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`shipment_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
```

Feed this file into your chosen tool → get Postgres or Snowflake DDL out.

---

## For Your Current Project (Quick Path)

Since you only have 2 tables right now, the fastest path is:

```bash
# 1. Export MySQL DDL
docker exec flink-self-traning-mysql-1 mysqldump -uroot -p123456 \
    --no-data --skip-comments db_1 > mysql-schema.sql

# 2. Convert (pick one):

# Option A: Use SCT desktop app (GUI, visual)
# Option B: Quick manual conversion for 2 tables is fine
# Option C: Use an online converter like https://www.jooq.org/translate/
#           (paste MySQL DDL → select Postgres → get converted DDL)

# 3. Apply to Postgres
docker exec -i postgres psql -U postgres -d pgdb < postgres-schema.sql
```

When you scale to 50 tables, that's when SCT or SnowConvert saves real time.

## TL;DR

| Question | Answer |
|----------|--------|
| **What do these tools do?** | Convert DDL (CREATE TABLE, etc.) from one database dialect to another |
| **AWS SCT** | Free desktop GUI, supports many source/target combinations |
| **SnowConvert** | Web + CLI, specifically for migrating to Snowflake |
| **Do they migrate data?** | No (just schema). Use DMS, pgLoader, or your Flink CDC pipeline for data |
| **Do I need them for 2 tables?** | Not really — manual conversion is fine. Worth it at 10+ tables |
| **How to get MySQL DDL?** | `mysqldump --no-data db_name > schema.sql` |
| **Key gotcha** | Verify type mappings match what Debezium sends (especially `TINYINT(1)` → `BOOLEAN`) |
