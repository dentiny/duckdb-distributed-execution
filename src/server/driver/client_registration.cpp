#include "server/driver/client_registration.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "server/driver/distributed_executor.hpp"
#include "server/driver/worker_manager.hpp"
#include "utils/time_utils.hpp"

namespace duckdb {

ClientRegistration::ClientRegistration(DuckDB &db, WorkerManager &worker_manager, distributed::ClientRole role_p)
    : role(role_p), last_seen(GetSteadyNowMilliSecSinceEpoch()), connection(make_uniq<Connection>(db)) {
	auto use_result = connection->Query("USE duckling;");
	if (use_result->HasError()) {
		throw InternalException(
		    StringUtil::Format("Failed to USE duckling for client connection: %s", use_result->GetError()));
	}
	distributed_executor = make_uniq<DistributedExecutor>(worker_manager, *connection);
}

ClientRegistration::~ClientRegistration() = default;

} // namespace duckdb
