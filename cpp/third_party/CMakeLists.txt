include(FetchContent)

FetchContent_Declare(
  pybind11
  GIT_REPOSITORY https://github.com/pybind/pybind11.git
  GIT_TAG          v2.11.1
)

FetchContent_Declare(
  gflags
  GIT_REPOSITORY https://github.com/gflags/gflags
  GIT_TAG        v2.2.2
)

FetchContent_MakeAvailable(pybind11 gflags)
