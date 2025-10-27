#include "distributed_server.hpp"
#include <iostream>
#include <string>

int main() {
	std::cout << "Starting Distributed DuckDB Server..." << std::endl;

	try {
		duckdb::DistributedServer server;

		// Interactive mode for testing
		std::string sql;
		std::cout << "Server ready. Enter SQL queries (or 'quit' to exit):" << std::endl;

		while (std::getline(std::cin, sql)) {
			if (sql == "quit" || sql == "exit") {
				break;
			}

			if (sql.empty()) {
				continue;
			}

			auto result = server.ExecuteQuery(sql);
			if (result->HasError()) {
				std::cout << "Error: " << result->GetError() << std::endl;
			} else {
				result->Print();
			}
		}

	} catch (const std::exception &e) {
		std::cerr << "Server error: " << e.what() << std::endl;
		return 1;
	}

	std::cout << "Server shutting down..." << std::endl;
	return 0;
}
