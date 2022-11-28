include(FetchContent)

FetchContent_Declare(
  gflags
  GIT_REPOSITORY https://github.com/gflags/gflags
  GIT_TAG        v2.2.2
)

FetchContent_Declare(
  nanodbc
  GIT_REPOSITORY https://github.com/nanodbc/nanodbc.git
  GIT_TAG        v2.14.0
)

set(NANODBC_DISABLE_TESTS ON)
FetchContent_MakeAvailable(gflags nanodbc)
