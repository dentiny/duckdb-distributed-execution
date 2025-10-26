#pragma once

#include <tuple>

#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

struct EntryLookupInfoKey {
	CatalogType type;
	string name;
};

struct EntryLookupInfoEqual {
	bool operator()(const EntryLookupInfoKey &lhs, const EntryLookupInfoKey &rhs) const;
};

struct EntryLookupInfoHash {
	std::size_t operator()(const EntryLookupInfoKey &key) const;
};

} // namespace duckdb
