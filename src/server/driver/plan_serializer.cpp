#include "server/driver/plan_serializer.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckherder_extension.hpp"

namespace duckdb {

string PlanSerializer::SerializeLogicalPlan(LogicalOperator &op) {
	MemoryStream stream;
	BinarySerializer serializer(stream);
	serializer.Begin();
	op.Serialize(serializer);
	serializer.End();
	auto data_ptr = stream.GetData();
	return string(reinterpret_cast<const char *>(data_ptr), stream.GetPosition());
}

unique_ptr<LogicalOperator> PlanSerializer::DeserializeLogicalPlan(ClientContext &context, const string &data) {
	unique_ptr<LogicalOperator> plan;
	// Table scans look up their tables by name, which needs a transaction.
	context.RunFunctionInTransaction([&]() {
		MemoryStream stream(reinterpret_cast<data_ptr_t>(const_cast<char *>(data.data())), data.size());
		bound_parameter_map_t parameters;
		plan = BinaryDeserializer::Deserialize<LogicalOperator>(stream, context, parameters);
	});
	if (!plan) {
		throw SerializationException("Deserialized plan is empty");
	}
	plan->ResolveOperatorTypes();
	return plan;
}

string PlanSerializer::BuildId() {
	return StringUtil::Format("%s/%s", DuckDB::SourceID(), DuckherderExtension().Version());
}

} // namespace duckdb
