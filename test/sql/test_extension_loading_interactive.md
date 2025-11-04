# Interactive SQL Testing Guide for Extension Loading

## Prerequisites

1. **Build the extension**:
```bash
cd /home/vscode/duckdb-distributed-execution
cd build/reldebug
make -j
```

2. **Start the distributed server** (in Terminal 1):
```bash
cd /home/vscode/duckdb-distributed-execution/build/reldebug
./distributed_server --host localhost --port 8815
```

You should see output like:
```
Server started on localhost:8815
```

## Testing Steps

### Open DuckDB Client (in Terminal 2):

```bash
cd /home/vscode/duckdb-distributed-execution
./duckdb
```

---

## Test 1: Basic Extension Loading

### Step 1: Load the duckherder extension
```sql
LOAD './build/reldebug/extension/duckherder/duckherder.duckdb_extension';
```

**Expected**: No errors

---

### Step 2: Attach to distributed database
```sql
ATTACH DATABASE '' AS dh (TYPE duckherder, server_host 'localhost', server_port 8815);
```

**Expected**: Database attached successfully

---

### Step 3: Switch to distributed database
```sql
USE dh;
```

**Expected**: Database switched

---

### Step 4: Check current extensions
```sql
SELECT extension_name, loaded, installed 
FROM duckdb_extensions() 
WHERE extension_name = 'httpfs';
```

**Expected**: Shows httpfs as not loaded (or not present)

---

### Step 5: Load httpfs extension
```sql
LOAD httpfs;
```

**Expected**: 
- Command succeeds
- Check the server terminal - you should see log messages indicating the extension was loaded

**In the server terminal, you should see something like**:
```
Handling LOAD_EXTENSION request for: httpfs
Extension loaded successfully
```

---

### Step 6: Verify extension is loaded
```sql
SELECT extension_name, loaded, installed 
FROM duckdb_extensions() 
WHERE extension_name = 'httpfs';
```

**Expected**: Shows httpfs as loaded=true

---

## Test 2: Test with JSON Extension

### Load json extension
```sql
LOAD json;
```

**Expected**: Loads on both client and server

### Verify with JSON operations
```sql
SELECT '{"name": "test", "value": 42}'::JSON as json_data;
```

**Expected**: JSON parsing works

---

## Test 3: Install and Load from Version

### Install specific version (if available)
```sql
-- This may vary based on available extensions
INSTALL parquet;
LOAD parquet;
```

**Expected**: Extension installs and loads on both sides

### Verify
```sql
SELECT extension_name, loaded 
FROM duckdb_extensions() 
WHERE extension_name = 'parquet';
```

---

## Test 4: Create and Query Remote Table

### Create a table using the distributed database
```sql
CREATE TABLE users (id INTEGER, name VARCHAR, email VARCHAR);
```

### Insert data
```sql
INSERT INTO users VALUES 
    (1, 'Alice', 'alice@example.com'),
    (2, 'Bob', 'bob@example.com'),
    (3, 'Charlie', 'charlie@example.com');
```

### Query the data
```sql
SELECT * FROM users ORDER BY id;
```

**Expected**: Returns all 3 rows

### Clean up
```sql
DROP TABLE users;
```

---

## Test 5: Server Availability (Optional)

### Test 5a: Stop the server
1. Go to Terminal 1 (server terminal)
2. Press Ctrl+C to stop the server

### Test 5b: Try loading extension without server
```sql
LOAD fts;
```

**Expected**: 
- Command still succeeds (client-side load works)
- You may see a warning in the client about server unavailability
- This demonstrates graceful degradation

### Test 5c: Restart server
```bash
./distributed_server --host localhost --port 8815
```

---

## Verification Checklist

After running these tests, verify:

- ✅ Extensions load without errors
- ✅ `duckdb_extensions()` shows extensions as loaded
- ✅ Extension functionality works (e.g., JSON parsing, parquet reading)
- ✅ Server logs show extension loading requests
- ✅ Client works even when server is down (graceful degradation)
- ✅ Tables can be created and queried on distributed database

---

## Troubleshooting

### Error: "Extension not found"
**Solution**: Make sure the extension is available. Try:
```sql
SELECT extension_name, installed FROM duckdb_extensions() ORDER BY extension_name;
```

### Error: "Connection refused"
**Solution**: Check that the server is running:
```bash
ps aux | grep distributed_server
```

### Error: "Catalog not found"
**Solution**: Make sure you've loaded the duckherder extension and attached to it:
```sql
LOAD './build/reldebug/extension/duckherder/duckherder.duckdb_extension';
ATTACH DATABASE '' AS dh (TYPE duckherder);
USE dh;
```

### Server shows no log messages
**Solution**: Check the server was compiled with the new code:
```bash
cd /home/vscode/duckdb-distributed-execution/build/reldebug
make clean
make -j
```

---

## Advanced Testing

### Test with HTTP/S Extension
```sql
LOAD httpfs;

-- Try reading from a URL (requires internet)
CREATE TABLE test_http AS 
    SELECT * FROM read_csv_auto('https://raw.githubusercontent.com/datasets/airport-codes/master/data/airport-codes.csv')
    LIMIT 10;

SELECT * FROM test_http;

DROP TABLE test_http;
```

### Test with Multiple Extensions
```sql
-- Load multiple extensions
LOAD httpfs;
LOAD json;
LOAD parquet;

-- Verify all loaded
SELECT extension_name, loaded 
FROM duckdb_extensions() 
WHERE extension_name IN ('httpfs', 'json', 'parquet')
ORDER BY extension_name;
```

---

## Expected Output Examples

### Successful LOAD command:
```
┌─────────┐
│ Success │
├─────────┤
│ true    │
└─────────┘
```

### Extension list after loading:
```
┌────────────────┬────────┬───────────┐
│ extension_name │ loaded │ installed │
├────────────────┼────────┼───────────┤
│ httpfs         │ true   │ true      │
└────────────────┴────────┴───────────┘
```

### Server logs:
```
[2025-11-04 12:34:56] Handling DoAction request
[2025-11-04 12:34:56] Request type: LOAD_EXTENSION
[2025-11-04 12:34:56] Loading extension: httpfs
[2025-11-04 12:34:56] Extension loaded successfully
```

---

## Quick Test Script

Copy and paste this into the DuckDB client for a quick end-to-end test:

```sql
-- Quick test script
LOAD './build/reldebug/extension/duckherder/duckherder.duckdb_extension';
ATTACH DATABASE '' AS dh (TYPE duckherder, server_host 'localhost', server_port 8815);
USE dh;

-- Test extension loading
LOAD json;
SELECT '{"test": "passed"}'::JSON as result;

-- Test table operations
CREATE TABLE test_sync (id INT, value VARCHAR);
INSERT INTO test_sync VALUES (1, 'synchronized');
SELECT * FROM test_sync;
DROP TABLE test_sync;

-- Final status
SELECT 'All tests completed successfully!' as status;
```

---

## Next Steps

After confirming the feature works:

1. **Test with your own extensions**: Try loading extensions specific to your use case
2. **Test error scenarios**: Try loading non-existent extensions to verify error handling
3. **Performance testing**: Load multiple extensions and measure overhead
4. **Integration testing**: Use the extensions in complex queries across distributed tables

---

## Need Help?

Check the documentation:
- Feature overview: `/home/vscode/duckdb-distributed-execution/docs/EXTENSION_LOADING.md`
- Implementation details: `/home/vscode/duckdb-distributed-execution/IMPLEMENTATION_SUMMARY.md`
- Server logs for debugging: Check the terminal where you ran `distributed_server`

