#include "entry_lookup_info_hash_utils.hpp"

namespace duckdb {

bool EntryLookupInfoEqual::operator()(const EntryLookupInfoKey &lhs, const EntryLookupInfoKey &rhs) const {
	return lhs.type == rhs.type && lhs.name == rhs.name;
}

std::size_t EntryLookupInfoHash::operator()(const EntryLookupInfoKey &key) const {
	const std::size_t h1 = std::hash<int>()(static_cast<int>(key.type));
	const std::size_t h2 = std::hash<std::string>()(key.name);
	return h1 ^ (h2 << 1);
}

} // namespace duckdb
