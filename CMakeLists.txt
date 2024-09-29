cmake_minimum_required(VERSION 3.8)

project(KeyValueStore C CXX)

include("${CMAKE_CURRENT_SOURCE_DIR}/common.cmake")

# Find absl package
find_package(absl CONFIG REQUIRED)

# Find RocksDB
find_library(ROCKSDB_LIB rocksdb_static REQUIRED)

# Proto file
get_filename_component(kvs_proto "./proto/keyvaluestore.proto" ABSOLUTE)
get_filename_component(kvs_proto_path "${kvs_proto}" PATH)

# Generated sources
set(kvs_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/keyvaluestore.pb.cc")
set(kvs_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/keyvaluestore.pb.h")
set(kvs_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/keyvaluestore.grpc.pb.cc")
set(kvs_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/keyvaluestore.grpc.pb.h")

add_custom_command(
    OUTPUT "${kvs_proto_srcs}" "${kvs_proto_hdrs}" "${kvs_grpc_srcs}" "${kvs_grpc_hdrs}"
    COMMAND ${_PROTOBUF_PROTOC}
    ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
    --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
    -I "${kvs_proto_path}"
    --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
    "${kvs_proto}"
    DEPENDS "${kvs_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/server")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/client")

# kvs_grpc_proto
add_library(kvs_grpc_proto
  ${kvs_grpc_srcs}
  ${kvs_grpc_hdrs}
  ${kvs_proto_srcs}
  ${kvs_proto_hdrs})
target_link_libraries(kvs_grpc_proto
  absl::absl_log
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

# RocksDB wrapper
add_library(rocksdb_wrapper
  server/rocksdb_wrapper.cc)
target_link_libraries(rocksdb_wrapper
  ${ROCKSDB_LIB} -static-libstdc++ -static-libgcc)

# Set compiler flags for position-independent code (PIC) for building shared libraries
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

# Create shared library lib739kv.so from lib739kv.cc and lib739kv.h
add_library(739kv SHARED
  client/lib739kv.cc)
target_link_libraries(739kv
  kvs_grpc_proto
  absl::flags_parse
  absl::log_initialize
  absl::log_globals)

# Targets keyvaluestore_client
add_executable(keyvaluestore_client
  client/keyvaluestore_client.cc)
target_link_libraries(keyvaluestore_client
  739kv)

# Targets keyvaluestore_server
add_executable(keyvaluestore_server
  server/keyvaluestore_server.cc)
target_link_libraries(keyvaluestore_server
  kvs_grpc_proto
  rocksdb_wrapper
  absl::flags_parse
  absl::log_initialize
  absl::log_globals)