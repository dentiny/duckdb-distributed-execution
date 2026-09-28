#pragma once

#include "duckdb/catalog/duck_catalog.hpp"

namespace duckdb {

class DucklingCatalog : public DuckCatalog {
public:
	using DuckCatalog::DuckCatalog;

	string GetCatalogType() override {
		return "duckling";
	}
};

} // namespace duckdb
