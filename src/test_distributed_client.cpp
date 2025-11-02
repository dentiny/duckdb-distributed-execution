#include "distributed_flight_client.hpp"
#include "distributed_protocol.hpp"
#include <iostream>
#include <arrow/api.h>

using namespace duckdb;

int main(int argc, char *argv[]) {
	std::string server_url = "grpc://localhost:8815";

	if (argc > 1) {
		server_url = argv[1];
	}

	std::cout << "🔌 Connecting to server: " << server_url << std::endl;

	try {
		// ===================================================================
		// STEP 1: Create client and connect
		// ===================================================================
		DistributedFlightClient client(server_url);
		auto status = client.Connect();
		if (!status.ok()) {
			std::cerr << "❌ Connection failed: " << status.ToString() << std::endl;
			return 1;
		}
		std::cout << "✅ Connected to server" << std::endl;

		// ===================================================================
		// STEP 2: Create table using protobuf (client sends CreateTableRequest)
		// ===================================================================
		std::cout << "\n📝 Creating table 'users'..." << std::endl;

		distributed::DistributedResponse create_response;
		status = client.CreateTable("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)", create_response);

		if (!status.ok()) {
			std::cerr << "❌ RPC failed: " << status.ToString() << std::endl;
			return 1;
		}

		if (!create_response.success()) {
			std::cerr << "❌ Server error: " << create_response.error_message() << std::endl;
			return 1;
		}

		std::cout << "✅ Table created successfully" << std::endl;
		// create_response.has_create_table() is true here

		// ===================================================================
		// STEP 3: Check if table exists (client sends TableExistsRequest)
		// ===================================================================
		std::cout << "\n🔍 Checking if table exists..." << std::endl;

		bool exists = false;
		status = client.TableExists("users", exists);

		if (!status.ok()) {
			std::cerr << "❌ RPC failed: " << status.ToString() << std::endl;
			return 1;
		}

		std::cout << "Table exists: " << (exists ? "✅ YES" : "❌ NO") << std::endl;

		// ===================================================================
		// STEP 4: Insert some test data (client sends SQL via ExecuteSQLRequest)
		// ===================================================================
		std::cout << "\n💾 Inserting test data..." << std::endl;

		distributed::DistributedResponse insert_response;
		status = client.ExecuteSQL("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)",
		                           insert_response);

		if (!status.ok()) {
			std::cerr << "❌ RPC failed: " << status.ToString() << std::endl;
			return 1;
		}

		if (!insert_response.success()) {
			std::cerr << "❌ Server error: " << insert_response.error_message() << std::endl;
			return 1;
		}

		std::cout << "✅ Data inserted" << std::endl;

		// ===================================================================
		// STEP 5: Scan table and get Arrow RecordBatches (client sends ScanTableRequest)
		// ===================================================================
		std::cout << "\n📊 Scanning table (returns Arrow RecordBatches)..." << std::endl;

		std::unique_ptr<arrow::flight::FlightStreamReader> stream;
		status = client.ScanTable("users", 100, 0, stream);

		if (!status.ok()) {
			std::cerr << "❌ Scan failed: " << status.ToString() << std::endl;
			return 1;
		}

		std::cout << "✅ Got Arrow stream from server" << std::endl;

		// Read Arrow RecordBatches from stream
		uint64_t total_rows = 0;
		int batch_count = 0;

		while (true) {
			auto result = stream->Next();
			if (!result.ok()) {
				std::cerr << "❌ Stream error: " << result.status().ToString() << std::endl;
				break;
			}

			auto batch_with_metadata = result.ValueOrDie();
			if (!batch_with_metadata.data) {
				break; // End of stream
			}

			auto batch = batch_with_metadata.data;
			batch_count++;
			total_rows += batch->num_rows();

			std::cout << "\n📦 Batch " << batch_count << ":" << std::endl;
			std::cout << "  Rows: " << batch->num_rows() << std::endl;
			std::cout << "  Columns: " << batch->num_columns() << std::endl;

			// Print batch content
			std::cout << "  Data:" << std::endl;
			std::cout << batch->ToString() << std::endl;
		}

		std::cout << "\n✅ Scan complete!" << std::endl;
		std::cout << "📊 Total batches: " << batch_count << std::endl;
		std::cout << "📊 Total rows: " << total_rows << std::endl;

		// ===================================================================
		// STEP 6: Drop table (client sends DropTableRequest)
		// ===================================================================
		std::cout << "\n🗑️  Dropping table..." << std::endl;

		distributed::DistributedResponse drop_response;
		status = client.DropTable("users", drop_response);

		if (!status.ok()) {
			std::cerr << "❌ RPC failed: " << status.ToString() << std::endl;
			return 1;
		}

		if (!drop_response.success()) {
			std::cerr << "❌ Server error: " << drop_response.error_message() << std::endl;
			return 1;
		}

		std::cout << "✅ Table dropped" << std::endl;

		std::cout << "\n🎉 All operations completed successfully!" << std::endl;

	} catch (const std::exception &ex) {
		std::cerr << "❌ Fatal error: " << ex.what() << std::endl;
		return 1;
	}

	return 0;
}
