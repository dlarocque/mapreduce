cmake_minimum_required(VERSION 3.27)
project(mapreduce)

set(CMAKE_CXX_STANDARD 23)

# Manually set the path to the gRPC and Protobuf binaries
list(APPEND CMAKE_PREFIX_PATH "$ENV{HOME}/.local")

# Find Protobuf installation
find_package(Protobuf REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}")

find_package(gRPC REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

find_program(gRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)

# Proto file
set(PROTO_PATH "${CMAKE_CURRENT_SOURCE_DIR}/src/protos")
set(PROTO_NAME "mapreduce")

set(Protobuf_PROTOC_EXECUTABLE $ENV{HOME}/.local/bin/protoc)
set(gRPC_CPP_PLUGIN $ENV{HOME}/.local/bin/grpc_cpp_plugin)

# Generate gRPC code
set(GENERATED_PROTOBUF_PATH "${CMAKE_CURRENT_BINARY_DIR}/generated")
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
        COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        ARGS --grpc_out=${GENERATED_PROTOBUF_PATH}
        --cpp_out=${GENERATED_PROTOBUF_PATH}
        -I ${PROTO_PATH}
        --plugin=protoc-gen-grpc=${gRPC_CPP_PLUGIN}
        ${PROTO_PATH}/${PROTO_NAME}.proto
        DEPENDS ${PROTO_PATH}/${PROTO_NAME}.proto
)

include_directories(${CMAKE_BINARY_DIR}/generated)

add_library(mapreduce STATIC include/mapreduce.cpp
        src/worker.cpp
        src/worker.h
        src/coordinator.cpp
        src/coordinator.h
        ${GENERATED_PROTOBUF_FILES})
target_include_directories(mapreduce PUBLIC include ${Protobuf_INCLUDE_DIRS})

# Examples
add_executable(wc examples/wc.cpp)
target_link_libraries(
        wc
        mapreduce
        ${Protobuf_LIBRARIES}
        gRPC::grpc++
        gRPC::grpc++_reflection
)
