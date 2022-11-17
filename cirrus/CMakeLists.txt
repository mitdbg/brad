cmake_minimum_required(VERSION 3.17)
project(cirrus LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_subdirectory(third_party)

# All Cirrus utilities.
add_library(cirrus STATIC)
target_include_directories(cirrus PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(cirrus PUBLIC nanodbc gflags)

add_subdirectory(datasets)
add_subdirectory(utils)
add_subdirectory(workloads)

# Benchmark executables.
add_executable(tpch_s3_loader exe/tpch_s3.cc)
target_link_libraries(tpch_s3_loader PRIVATE cirrus)

add_executable(tpch_scale exe/tpch_scale.cc)
target_link_libraries(tpch_scale PRIVATE cirrus)

add_executable(store_gen exe/store_gen.cc)
target_link_libraries(store_gen PRIVATE cirrus)

add_executable(store_sale exe/store_sale.cc)
target_link_libraries(store_sale PRIVATE cirrus)
