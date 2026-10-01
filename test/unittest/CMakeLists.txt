include_directories(include)
include_directories(${CMAKE_SOURCE_DIR}/third_party)
include_directories(${CMAKE_SOURCE_DIR}/src/include)
include_directories(${DuckDB_SOURCE_DIR}/third_party)
include_directories(${DuckDB_SOURCE_DIR}/test/include)

set(DUCKHERDER_UNITTEST_SOURCES
    main.cpp
    test_arrow_enum_conversion.cpp
    test_arrow_list_array_conversion.cpp
    test_arrow_map_conversion.cpp
    test_arrow_struct_conversion.cpp
    test_distributed_flight.cpp
    test_arrow_uuid_conversion.cpp
    test_duckherder_remote_endpoint.cpp
    test_query_plan_analyzer.cpp
    test_remote_error.cpp)

add_executable(unittest_duckherder ${DUCKHERDER_UNITTEST_SOURCES})

# Match DuckDB's test runner: link the core before the generated loader and
# include the extension archives that provide Duckherder's internal symbols.
target_link_libraries(unittest_duckherder duckdb_static test_helpers)
link_extension_libraries(unittest_duckherder "")

target_link_libraries(
  unittest_duckherder Arrow::arrow_static ArrowFlight::arrow_flight_static
  protobuf::libprotobuf gRPC::grpc++)
