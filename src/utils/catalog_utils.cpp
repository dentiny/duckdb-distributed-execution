#include "utils/catalog_utils.hpp"

namespace duckdb {

string SanitizeQuery(const string &sql, const string &catalog_name) {
	string result = sql;
	string catalog_prefix = catalog_name + ".";
	size_t pos = 0;
	while ((pos = result.find(catalog_prefix, pos)) != string::npos) {
		result.erase(pos, catalog_prefix.length());
		// Don't increment pos since we just erased characters.
	}
	return result;
}

}  // namespace duckdb
