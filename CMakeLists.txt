cmake_minimum_required(VERSION 3.27)
project(mapreduce)

set(CMAKE_CXX_STANDARD 23)


add_library(mapreduce STATIC include/mapreduce.cpp)
target_include_directories(mapreduce PUBLIC include)

# Examples
add_executable(wc examples/wc.cpp)
target_link_libraries(wc mapreduce)
