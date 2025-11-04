# Distributed Extension Loading

## Overview

The DuckHerder extension now supports automatic extension loading synchronization between client and server. When you load an extension on the client side using the `LOAD` statement, it will automatically be loaded on the server side as well.

## How It Works

1. **Client-Side Interception**: When you execute a `LOAD` statement on a client connected to a DuckHerder database, the extension intercepts the statement during the binding phase.

2. **Dual Execution**: The extension is loaded both:
   - Locally on the client side (for client-side operations)
   - Remotely on the server side (via RPC call)

3. **Server-Side Loading**: The server receives a `LOAD_EXTENSION` request and executes the corresponding `LOAD` or `INSTALL` statement in its database instance.

## Usage Examples

### Basic Extension Loading

```sql
-- Attach to a DuckHerder database
ATTACH DATABASE '' AS duckherder (TYPE duckherder, server_host 'localhost', server_port 8815);
USE duckherder;

-- Load an extension - this will load on both client and server
LOAD httpfs;

-- Now you can use the extension on both sides
SELECT * FROM read_parquet('https://example.com/data.parquet');
```

### Installing Extensions

```sql
-- Install and load an extension from a specific version
INSTALL httpfs VERSION '1.0.0';
LOAD httpfs;

-- Or install from a specific repository
INSTALL my_extension FROM 'https://my-repo.com/extensions';
LOAD my_extension;
```

## Architecture

### Components

1. **bind_load_override.cpp**: Overrides DuckDB's default `LOAD` statement binding to detect when DuckHerder catalog is in use.

2. **LogicalDistributedLoad**: A custom logical operator that represents a distributed load operation.

3. **PhysicalDistributedLoad**: A physical operator that:
   - Executes the load locally (using DuckDB's standard mechanism)
   - Sends a load request to the server
   - Handles errors gracefully (server failures don't fail the client operation)

4. **DistributedFlightClient::LoadExtension**: Client-side RPC method that sends load requests to the server.

5. **DistributedFlightServer::HandleLoadExtension**: Server-side handler that processes load requests and executes the corresponding SQL.

### Protocol Buffer Definition

```protobuf
message LoadExtensionRequest {
  string extension_name = 1;  // Extension name or path
  string repository = 2;       // Optional repository
  string version = 3;          // Optional version
}

message LoadExtensionResponse {}
```

## Error Handling

The implementation is designed to be resilient:

- If the server is unavailable, the client-side load still succeeds
- Server-side load failures are logged but don't fail the client operation
- This ensures that development workflows aren't disrupted by server issues

## Testing

### Manual Testing

1. **Start the server**:
```bash
./distributed_server --host localhost --port 8815 --db-path /tmp/server.db
```

2. **Connect from client**:
```sql
-- In DuckDB client
LOAD './build/reldebug/extension/duckherder/duckherder.duckdb_extension';
ATTACH DATABASE '' AS dh (TYPE duckherder, server_host 'localhost', server_port 8815);
USE dh;

-- Load an extension
LOAD httpfs;

-- Verify it works on both sides
SELECT * FROM duckdb_extensions() WHERE extension_name = 'httpfs';
```

3. **Verify on server**:
Check the server logs to confirm the extension was loaded.

### Expected Behavior

- ✅ Extension loads successfully on client
- ✅ Extension loads successfully on server
- ✅ Both client and server can use the extension's features
- ✅ Client operations succeed even if server is unavailable
- ✅ Error messages are informative

## Limitations

1. **Extension Availability**: The extension must be available on both client and server. If an extension is only available on the client, the server-side load will fail (but client-side will succeed).

2. **Version Matching**: The extension version should ideally match between client and server to ensure consistent behavior.

3. **Custom Extensions**: Extensions with custom paths (e.g., `LOAD '/path/to/ext.duckdb_extension'`) will only work if the path is valid on the server side.

## Implementation Notes

### Why Override bind_load.cpp?

DuckDB's binder doesn't provide a virtual method for catalogs to override `LOAD` statement binding. To intercept `LOAD` statements, we override the `Binder::Bind(LoadStatement&)` method in our extension. This file is compiled before DuckDB's version, allowing our implementation to take precedence.

### Future Improvements

1. **Extension Verification**: Add checksums or signatures to verify extension integrity across client and server.

2. **Automatic Sync**: Periodically sync loaded extensions between client and server.

3. **Configuration Options**: Allow users to disable auto-loading on server side if desired.

4. **Better Error Reporting**: Provide more detailed information about why server-side loads fail.

## Troubleshooting

### Extension loads on client but not on server

**Symptoms**: You can use the extension locally, but server-side operations fail.

**Solutions**:
1. Check server logs for error messages
2. Verify the extension is installed on the server
3. Ensure the server has necessary permissions to load extensions

### Version mismatch errors

**Symptoms**: Extension behaves differently on client vs server.

**Solutions**:
1. Specify explicit versions: `INSTALL httpfs VERSION '1.0.0'`
2. Ensure both client and server use the same DuckDB version
3. Reinstall extensions on both sides

### Server unreachable warnings

**Symptoms**: Warnings in client logs about server connection failures.

**Solutions**:
1. Verify server is running: check process list
2. Check network connectivity: `ping <server_host>`
3. Verify firewall rules allow connections on the server port
4. These warnings are informational - client-side operations will still work

