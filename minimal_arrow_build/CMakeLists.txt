cmake_minimum_required(VERSION 3.16)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# set(CMAKE_PREFIX_PATH "/opt/homebrew/")

project(ArrowMinimalExample)

find_package(Arrow REQUIRED)
find_package(gflags REQUIRED)

add_executable(arrow_example arrow_example.cc)
target_link_libraries(arrow_example PRIVATE Arrow:arrow_shared gflags)
