# Distributed Extension Loading

## Overview

The DuckHerder extension provides a function to load extensions on the server side. After loading an extension locally with `LOAD`, you can use `SELECT duckherder_load_extension('extension_name')` to load it on the server as well.

## Usage

### Basic Extension Loading

```sql
-- Attach to a DuckHerder database
ATTACH DATABASE '' AS dh (TYPE duckherder, server_host 'localhost', server_port 8815);
USE dh;

-- Step 1: Load extension on client
LOAD httpfs;

-- Step 2: Load extension on server
SELECT duckherder_load_extension('httpfs');

-- Now the extension is available on both client and server!
```

### Quick Example

```sql
-- Load duckherder extension
LOAD './build/reldebug/extension/duckherder/duckherder.duckdb_extension';

-- Connect to distributed database
ATTACH DATABASE '' AS dh (TYPE duckherder, server_host 'localhost', server_port 8815);
USE dh;

-- Load json extension everywhere
LOAD json;
SELECT duckherder_load_extension('json');

-- Test it works
SELECT '{"name": "test", "value": 42}'::JSON as data;
```

## Server-Side Logging

When you call `duckherder_load_extension`, the server will output detailed logs:

```
[SERVER] Received request: LOAD_EXTENSION
[SERVER] ========================================
[SERVER] Extension Load Request Received
[SERVER] Extension: httpfs
[SERVER] Executing: LOAD httpfs
[SERVER] ✅ Extension 'httpfs' loaded successfully!
[SERVER] ========================================
```

## Error Handling

The function will throw an exception if:
- The server is not reachable
- The extension fails to load on the server  
- The duckherder database is not attached

```sql
-- This will throw an error if server is down
SELECT duckherder_load_extension('httpfs');
-- Error: Failed to connect to server: Connection refused
```

## Why Use a Function?

We use a function instead of automatic synchronization because:

1. **Explicit control**: You decide when to sync extensions
2. **No symbol conflicts**: Avoids overriding DuckDB internals
3. **Simple implementation**: Clean, maintainable code
4. **Error visibility**: Clear error messages when sync fails

## Complete Workflow

```sql
-- 1. Setup
LOAD './build/reldebug/extension/duckherder/duckherder.duckdb_extension';
ATTACH DATABASE '' AS dh (TYPE duckherder, server_host 'localhost', server_port 8815);
USE dh;

-- 2. Load extensions you need
LOAD httpfs;
LOAD json;
LOAD parquet;

-- 3. Sync to server
SELECT duckherder_load_extension('httpfs');
SELECT duckherder_load_extension('json');
SELECT duckherder_load_extension('parquet');

-- 4. Use your extensions
SELECT * FROM read_parquet('https://example.com/data.parquet');
```

## Testing

### Terminal 1 - Start Server:
```bash
cd /home/vscode/duckdb-distributed-execution/build/reldebug
./distributed_server
```

### Terminal 2 - Test Client:
```bash
./duckdb
```

Then run:
```sql
LOAD './extension/duckherder/duckherder.duckdb_extension';
ATTACH DATABASE '' AS dh (TYPE duckherder);
USE dh;

LOAD httpfs;
SELECT duckherder_load_extension('httpfs');

-- Check server terminal - you should see the logs!
```

## Available Functions

The duckherder extension provides these functions:

| Function | Arguments | Returns | Description |
|----------|-----------|---------|-------------|
| `duckherder_load_extension` | `extension_name` | `BOOLEAN` | Load extension on server |
| `duckherder_register_remote_table` | `table_name, remote_table` | Register remote table |
| `duckherder_unregister_remote_table` | `table_name` | Unregister remote table |

## Architecture

```
Client Side:
┌─────────────────────┐
│ LOAD httpfs;        │  ← Load locally
└─────────────────────┘
           │
           ▼
┌───────────────────────────────────────────────┐
│ SELECT duckherder_load_extension('httpfs');  │
└───────────────────────────────────────────────┘
           │
           ▼
┌────────────────────────────┐
│ DuckherderPragmas::        │
│ LoadExtension()            │
└────────────────────────────┘
           │
           ▼
┌────────────────────────────┐
│ DistributedFlightClient::  │
│ LoadExtension()            │  ← RPC Call
└────────────────────────────┘
           │
           ▼ (network)
Server Side:
┌────────────────────────────┐
│ DistributedFlightServer::  │
│ HandleLoadExtension()      │
└────────────────────────────┘
           │
           ▼
┌────────────────────────────┐
│ conn->Query("LOAD httpfs") │  ← Execute on server
└────────────────────────────┘
```

## Troubleshooting

### "Duckherder database 'dh' not attached"
Make sure you've attached the database:
```sql
ATTACH DATABASE '' AS dh (TYPE duckherder, server_host 'localhost', server_port 8815);
```

### "Failed to connect to server"
Check that the server is running:
```bash
ps aux | grep distributed_server
```

### "Server failed to load extension"
Check the server terminal for detailed error messages. The extension might not be available on the server.

## Limitations

1. Must manually call the function after each `LOAD`
2. Extension must be available on both client and server
3. Currently hardcoded to use database name "dh"

## Future Improvements

- Auto-sync option to automatically call function after LOAD
- Support for extension repositories and versions
- Batch loading of multiple extensions
- Status query to check which extensions are loaded on server

