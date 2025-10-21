#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "motherduck_table_metadata.hpp"

namespace duckdb {

MotherduckTableMetadata::MotherduckTableMetadata(const string &schema, const string &table)
    : schema(schema), table(table) {
}

} // namespace duckdb
