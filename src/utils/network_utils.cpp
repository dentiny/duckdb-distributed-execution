#include "utils/network_utils.hpp"

#include <cstdint>
#include <cstring>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/socket.h>

namespace duckdb {

namespace {
constexpr uint64_t MAX_ATTEMPT_COUNT = 10;
} // namespace

int GetAvailablePort(int start_port) {
	for (int attempt = 0; attempt < MAX_ATTEMPT_COUNT; ++attempt) {
		int test_port = start_port + attempt;

		// Create a socket.
		int sock = socket(AF_INET, SOCK_STREAM, 0);
		if (sock < 0) {
			continue;
		}

		// Enable SO_REUSEADDR to quickly reuse ports.
		int reuse = 1;
		setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

		// Try to bind to the port.
		struct sockaddr_in addr;
		std::memset(&addr, 0, sizeof(addr));
		addr.sin_family = AF_INET;
		addr.sin_addr.s_addr = INADDR_ANY;
		addr.sin_port = htons(test_port);

		int bind_result = bind(sock, (struct sockaddr *)&addr, sizeof(addr));
		close(sock);

		// Port is available.
		if (bind_result == 0) {
			return test_port;
		}
		// Port is in use, try next one.
	}

	// No available port found
	return -1;
}

} // namespace duckdb
