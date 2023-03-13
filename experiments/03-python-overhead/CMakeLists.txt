cmake_minimum_required(VERSION 3.17)
project(iohtap_overhead LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

# Allows us to set CMake project options for subprojects that we include.
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

# Compile with architecture-specific instructions.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")

include(FetchContent)

# For configuration.
FetchContent_Declare(
  gflags
  GIT_REPOSITORY https://github.com/gflags/gflags
  GIT_TAG        v2.2.2
)
FetchContent_MakeAvailable(gflags)

# For ODBC connections.
FetchContent_Declare(
  nanodbc
  GIT_REPOSITORY https://github.com/nanodbc/nanodbc.git
  GIT_TAG        v2.14.0
)
set(NANODBC_DISABLE_TESTS ON)
FetchContent_MakeAvailable(nanodbc)

add_executable(native_overhead main.cc)
target_link_libraries(native_overhead PRIVATE nanodbc gflags)
