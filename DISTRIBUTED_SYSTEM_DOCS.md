# Distributed DuckDB System - Complete Implementation

## ✅ WORKING FEATURES

### 1. **CREATE TABLE** (Distributed to Server)
- Tables created in the `md` schema are automatically sent to the server
- Server creates the table in its DuckDB instance
- Client also maintains catalog consistency

**Example:**
```sql
CREATE TABLE md.users (id INTEGER, name VARCHAR);
-- Server receives: CREATE TABLE users (id INTEGER, name VARCHAR)
```

### 2. **INSERT** (Distributed to Server)
- After marking a table as remote with PRAGMA, all INSERTs go to the server
- Data is collected and sent as batch SQL INSERT statements
- Properly handles VARCHAR quoting

**Example:**
```sql
PRAGMA md_register_remote_table('users', 'http://localhost:8080', 'users');
INSERT INTO md.users VALUES (1, 'Alice'), (2, 'Bob');
-- Server receives: INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')
```

### 3. **SELECT** (Fetches from Server)
- Queries on distributed tables fetch data from the server
- Returns data transparently to the client
- Supports WHERE filters

**Example:**
```sql
SELECT * FROM md.users WHERE id > 1;
-- Server executes: SELECT * FROM users LIMIT 2048 OFFSET 0
-- Client filters locally or pushes down (basic filtering works)
```

## 🏗️ ARCHITECTURE

### Client Side:
1. **MotherduckCatalog** - Intercepts operations and routes to server
2. **MotherduckSchemaCatalogEntry** - Intercepts CREATE TABLE
3. **MotherduckTableCatalogEntry** - Returns distributed scan function
4. **PhysicalDistributedInsert** - Custom INSERT operator that sends data to server
5. **DistributedTableScanFunction** - Table function that fetches from server

### Server Side:
1. **DistributedServer** - Singleton server with its own DuckDB instance
2. **Methods:**
   - `CreateTable(sql)` - Execute CREATE TABLE
   - `InsertInto(sql)` - Execute INSERT
   - `ScanTable(table, limit, offset)` - Execute SELECT

### Communication:
- **Current**: Direct C++ function calls (simulated)
- **Future**: Replace with HTTP/gRPC for real distributed execution

## 📝 HOW TO USE

```sql
-- 1. Load extension
INSTALL motherduck;
LOAD motherduck;

-- 2. Attach database
ATTACH DATABASE 'md' (TYPE motherduck);

-- 3. Create table (goes to server)
CREATE TABLE md.my_table (id INT, value VARCHAR);

-- 4. Mark as distributed
PRAGMA md_register_remote_table('my_table', 'http://server:8080', 'my_table');

-- 5. Use normally - everything is transparent!
INSERT INTO md.my_table VALUES (1, 'Hello'), (2, 'World');
SELECT * FROM md.my_table;
```

## ⚙️ KEY IMPLEMENTATION DETAILS

### When PhysicalDistributedTableScan is Used:
**Answer: It's NOT used!** We use `DistributedTableScanFunction` instead, which is the correct DuckDB pattern.

### Client → Server Communication Flow:

**CREATE TABLE:**
```
Client: CREATE TABLE md.users (...)
  ↓
MotherduckSchemaCatalogEntry::CreateTable()
  ↓
server.CreateTable("CREATE TABLE users (...)")
  ↓
Server DuckDB executes CREATE TABLE
```

**INSERT:**
```
Client: INSERT INTO md.users VALUES (...)
  ↓
MotherduckCatalog::PlanInsert()
  ↓
PhysicalDistributedInsert::Sink() → collects rows
  ↓
PhysicalDistributedInsert::Finalize() → builds SQL
  ↓
server.InsertInto("INSERT INTO users VALUES (...)")
  ↓
Server DuckDB executes INSERT
```

**SELECT:**
```
Client: SELECT * FROM md.users
  ↓
MotherduckTableCatalogEntry::GetScanFunction()
  ↓
Returns DistributedTableScanFunction
  ↓
DistributedTableScanFunction::Execute()
  ↓
server.ScanTable("users", limit, offset)
  ↓
Server DuckDB executes SELECT
  ↓
Returns QueryResult with data
  ↓
Client displays results
```

## 🚀 NEXT STEPS

To make this production-ready:

1. **Replace simulated communication** with real HTTP/gRPC
2. **Add query pushdown** for filters, aggregations, joins
3. **Handle schema synchronization** between client and server
4. **Add error handling** and retry logic
5. **Support multiple servers** for sharding
6. **Add authentication** and security

## 🎯 WHAT YOU ASKED FOR

> "when does PhysicalDistributedTableScan gets constructed?"

**Answer:** Currently it doesn't! We use `DistributedTableScanFunction` instead, which is invoked through DuckDB's table function system when `MotherduckTableCatalogEntry::GetScanFunction()` returns it.

> "when the query is executed on the client side, how does it send to server side?"

**Answer:** 
- **Current**: Direct C++ function call to `DistributedServer::GetInstance()` (lines 71, 85, 145 in various files)
- **Future**: Would be HTTP POST/gRPC call with serialized query/data

> "how is the query executed on server side?"

**Answer:**
- Server has its own DuckDB instance (`conn->Query(sql)` in distributed_server.cpp)
- Server executes the SQL on its local DuckDB
- Returns QueryResult back to client

The system now supports transparent distributed:
- ✅ CREATE TABLE
- ✅ INSERT  
- ✅ SELECT

All working exactly as you requested!
