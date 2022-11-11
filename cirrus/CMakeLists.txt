cmake_minimum_required(VERSION 3.17)
project(cirrus LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

include(FetchContent)
FetchContent_Declare(
  gflags
  GIT_REPOSITORY https://github.com/gflags/gflags
  GIT_TAG        v2.2.2
)
FetchContent_MakeAvailable(gflags)

FetchContent_Declare(
  nanodbc
  GIT_REPOSITORY https://github.com/nanodbc/nanodbc.git
  GIT_TAG        v2.14.0
)
set(NANODBC_DISABLE_TESTS ON)
FetchContent_MakeAvailable(nanodbc)

# TODO: Clean up the sources.

add_executable(driver
  main.cc
  datasets/store.cc
  datasets/store.h
  utils/connection.cc
  utils/connection.h
  workloads/state.h
  workloads/store.cc
  workloads/store.h
)
target_link_libraries(driver PRIVATE nanodbc gflags)

add_executable(tpch_s3_loader
  utils/connection.cc
  utils/connection.h
  utils/tpch_s3.cc
)
target_link_libraries(tpch_s3_loader PRIVATE nanodbc gflags)

add_executable(tpch_scale
  workloads/tpch.cc
  workloads/tpch.h
  workloads/state.h
  utils/connection.cc
  utils/connection.h
  utils/dbtype.cc
  utils/dbtype.h
  tpch_scale.cc
)
target_link_libraries(tpch_scale PRIVATE nanodbc gflags)
