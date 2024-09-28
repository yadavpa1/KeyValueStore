cmake_minimum_required(VERSION 3.8)

project(KeyValueStore C CXX)

include("${CMAKE_CURRENT_SOURCE_DIR}/common.cmake")

# Find absl package
find_package(absl CONFIG REQUIRED)

# Proto file
get_filename_component(kvs_proto "./keyvaluestore.proto" ABSOLUTE)
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

# Add -fPIC for position-independent code [Needed for generating shared Library]
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

# kvs_grpc_proto - this will be the base library
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


# Create the client executable file for the end user

add_executable(keyvaluestore_client "keyvaluestore_client.cc")
target_link_libraries(keyvaluestore_client
    kvs_grpc_proto)



# # Create the shared library lib739kv.so
# add_library(lib739kv SHARED
#     "lib739kv.cc") 
# target_link_libraries(lib739kv
#     kvs_grpc_proto) 

# # Optionally, set the output name for the library explicitly (this will produce lib739kv.so)
# set_target_properties(lib739kv PROPERTIES OUTPUT_NAME "739kv")


# # Create the client executable file for the end user
# add_executable(kvClient "kvClient.cc")
# target_link_libraries(kvClient lib739kv)