include(FetchContent)

# For workload configuration.
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

# For specifying configuration options.
FetchContent_Declare(
  yaml-cpp
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
  GIT_TAG        yaml-cpp-0.6.3
)
set(YAML_CPP_BUILD_CONTRIB OFF)
set(YAML_CPP_BUILD_TOOLS OFF)
set(YAML_BUILD_SHARED_LIBS OFF)
set(YAML_CPP_BUILD_TESTS OFF)
set(YAML_CPP_INSTALL OFF)
FetchContent_MakeAvailable(yaml-cpp)

# For tests.
if(CR_BUILD_TESTS)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        release-1.11.0
  )
  FetchContent_MakeAvailable(googletest)
endif()

# Workload generator utilities from YCSBR.
add_subdirectory(ycsbr)
