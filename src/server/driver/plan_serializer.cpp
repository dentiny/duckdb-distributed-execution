#include "server/driver/plan_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"

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

string PlanSerializer::SerializeLogicalType(const LogicalType &type) {
	MemoryStream stream;
	BinarySerializer serializer(stream);
	serializer.Begin();
	type.Serialize(serializer);
	serializer.End();
	return string(reinterpret_cast<const char *>(stream.GetData()), stream.GetPosition());
}

} // namespace duckdb
