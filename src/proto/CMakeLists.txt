# Protobuf generation for distributed protocol
find_package(Protobuf REQUIRED)

set(PROTO_FILES
    distributed.proto
)

# Generate C++ sources from .proto files
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

# Create a library with generated protobuf code
add_library(distributed_proto STATIC ${PROTO_SRCS} ${PROTO_HDRS})

target_link_libraries(distributed_proto PUBLIC
    protobuf::libprotobuf
)

target_include_directories(distributed_proto PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR}  # For generated headers
)

# Export for parent CMakeLists.txt to use
set(DISTRIBUTED_PROTO_SOURCES ${PROTO_SRCS} PARENT_SCOPE)
set(DISTRIBUTED_PROTO_HEADERS ${PROTO_HDRS} PARENT_SCOPE)

