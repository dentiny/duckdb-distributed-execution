include_directories(include)
include_directories(${CMAKE_SOURCE_DIR}/third_party)
include_directories(${CMAKE_SOURCE_DIR}/src/include)
include_directories(${DuckDB_SOURCE_DIR}/third_party)
include_directories(${DuckDB_SOURCE_DIR}/test/include)

set(DUCKHERDER_UNITTEST_SOURCES
    main.cpp test_arrow_enum_conversion.cpp test_distributed_flight.cpp
    test_arrow_uuid_conversion.cpp test_duckherder_remote_endpoint.cpp
    test_query_plan_analyzer.cpp)

add_executable(unittest_duckherder ${DUCKHERDER_UNITTEST_SOURCES})

if(NOT WIN32
   AND NOT SUN
   AND NOT ZOS)
  target_link_libraries(unittest_duckherder test_helpers duckdb)
else()
  target_link_libraries(unittest_duckherder test_helpers duckdb_static)
endif()

target_link_libraries(
  unittest_duckherder Arrow::arrow_static ArrowFlight::arrow_flight_static
  protobuf::libprotobuf gRPC::grpc++)
