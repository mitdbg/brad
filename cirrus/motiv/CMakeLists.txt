cmake_minimum_required(VERSION 3.17)
project(cirrus LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

# Allows us to set CMake project options for subprojects that we include.
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

# Compile with architecture-specific instructions.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

option(CR_BUILD_TESTS "Set to build the tests." OFF)

# Third party dependencies.
add_subdirectory(third_party)

# We build Cirrus as a library (with an in-process API) to start. This
# eventually should become a service.
#
# NOTE: Right now the workload/dataset generator is entangled with Cirrus
# binary. This is done to support code reuse at the beginning of the project
# when we are hardcoding workload details into Cirrus. As Cirrus evolves to be
# more generalized, we should remove this entangling.
add_library(cirrus STATIC)
target_include_directories(cirrus
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(cirrus PUBLIC nanodbc yaml-cpp)

add_subdirectory(cirrus)
add_subdirectory(config)
add_subdirectory(datasets)
add_subdirectory(utils)
add_subdirectory(workloads)

if(CR_BUILD_TESTS)
  add_subdirectory(tests)
endif()
