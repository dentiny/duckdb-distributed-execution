#pragma once

#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

class MotherduckTransaction : public Transaction {
public:
	MotherduckTransaction(Catalog &catalog, TransactionManager &manager, ClientContext &context);

	~MotherduckTransaction();

	SchemaCatalogEntry &GetOrCreateSchema(const string &name);

private:
	Catalog &catalog;
	mutex lock;
	case_insensitive_map_t<unique_ptr<SchemaCatalogEntry>> schemas;
};

} // namespace duckdb
