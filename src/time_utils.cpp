#include "duckdb/common/types/timestamp.hpp"
#include "time_utils.hpp"

namespace duckdb {

time_t DuckdbTimestampToTimeT(timestamp_t timestamp) {
	auto components = Timestamp::GetComponents(timestamp);
	struct tm tm {};
	tm.tm_year = components.year - 1900;
	tm.tm_mon = components.month - 1;
	tm.tm_mday = components.day;
	tm.tm_hour = components.hour;
	tm.tm_min = components.minute;
	tm.tm_sec = components.second;
	tm.tm_isdst = 0;
	time_t result = mktime(&tm);
	return result;
}

} // namespace duckdb
