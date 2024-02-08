cmake_minimum_required(VERSION 3.8)
project(mapreduce)

set(CMAKE_CXX_STANDARD 23)

# Manually set the path to the gRPC and Protobuf binaries
list(APPEND CMAKE_PREFIX_PATH "$ENV{HOME}/.local")

find_package(Threads REQUIRED)

# Find Protobuf installation
option(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf REQUIRED CONFIG)
message(STATUS "Using protobuf ${Protobuf_VERSION}")

set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_REFLECTION gRPC::grpc++_reflection)
if(CMAKE_CROSSCOMPILING)
  find_program(_PROTOBUF_PROTOC protoc)
else()
  set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
endif()

# Find gRPC installation
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

set(_GRPC_GRPCPP gRPC::grpc++)
if(CMAKE_CROSSCOMPILING)
  find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
else()
  set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
endif()

# Proto file
set(PROTO_PATH "${CMAKE_CURRENT_SOURCE_DIR}/src/protos")
set(PROTO_NAME "coordinator")

# Generate gRPC code
set(GENERATED_PROTOBUF_PATH "${CMAKE_CURRENT_BINARY_DIR}")
file(MAKE_DIRECTORY ${GENERATED_PROTOBUF_PATH})
include_directories(${GENERATED_PROTOBUF_PATH})
set(GENERATED_PROTOBUF_FILES
        "${GENERATED_PROTOBUF_PATH}/${PROTO_NAME}.pb.cc"
        "${GENERATED_PROTOBUF_PATH}/${PROTO_NAME}.grpc.pb.cc"
        "${GENERATED_PROTOBUF_PATH}/${PROTO_NAME}.pb.h"
        "${GENERATED_PROTOBUF_PATH}/${PROTO_NAME}.grpc.pb.h"
        )

add_custom_command(
        OUTPUT ${GENERATED_PROTOBUF_FILES}
	COMMAND ${_PROTOBUF_PROTOC}
	ARGS --grpc_out=${CMAKE_CURRENT_BINARY_DIR}
	--cpp_out=${CMAKE_CURRENT_BINARY_DIR}
        -I ${PROTO_PATH}
        --plugin=protoc-gen-grpc=${_GRPC_CPP_PLUGIN_EXECUTABLE}
        ${PROTO_PATH}/${PROTO_NAME}.proto
        DEPENDS ${PROTO_PATH}/${PROTO_NAME}.proto
)

include_directories(${CMAKE_BINARY_DIR})
include_directories(${PROTOBUF_INCLUDE_DIRS})

# add_library(mapreduce STATIC include/mapreduce.cpp
#         src/worker.cpp
#         src/worker.h
#         src/coordinator.cpp
#         src/coordinator.h
#         ${GENERATED_PROTOBUF_FILES})
# target_include_directories(mapreduce PUBLIC include ${Protobuf_INCLUDE_DIRS})

add_library(hw_grpc_proto
        ${GENERATED_PROTOBUF_FILES}
        )
target_link_libraries(hw_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

add_executable(worker src/worker.cpp)
target_link_libraries(
        worker
	#         mapreduce
        hw_grpc_proto
        ${Protobuf_LIBRARIES}
        ${_REFLECTION}
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF})