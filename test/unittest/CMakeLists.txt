include_directories(include)
include_directories(${CMAKE_SOURCE_DIR}/third_party)
include_directories(${CMAKE_SOURCE_DIR}/src/include)
include_directories(${DuckDB_SOURCE_DIR}/third_party)
include_directories(${DuckDB_SOURCE_DIR}/test/include)

file(GLOB_RECURSE CATCHFS_UNITTEST_OBJECTS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

add_executable(unittest_duckherder main.cpp ${CATCHFS_UNITTEST_OBJECTS})

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
