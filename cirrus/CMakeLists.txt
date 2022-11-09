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

add_executable(driver
  main.cc
  connection.cc
  connection.h
  datasets/store.cc
  datasets/store.h
  workloads/state.h
  workloads/store.cc
  workloads/store.h
)
target_link_libraries(driver PRIVATE nanodbc gflags)
