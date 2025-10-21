#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "motherduck_schema.hpp"
#include "motherduck_transaction.hpp"

namespace duckdb {

MotherduckTransaction::MotherduckTransaction(Catalog &catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), catalog(catalog) {
}

MotherduckTransaction::~MotherduckTransaction() = default;

SchemaCatalogEntry &MotherduckTransaction::GetOrCreateSchema(const string &name) {
	lock_guard<mutex> guard(lock);
	if (auto it = schemas.find(name); it != schemas.end()) {
		return *it->second.get();
	}
	CreateSchemaInfo info;
	info.schema = name;
	schemas[name] = make_uniq<MotherduckSchema>(catalog, info);
	return *schemas[name];
}

} // namespace duckdb
