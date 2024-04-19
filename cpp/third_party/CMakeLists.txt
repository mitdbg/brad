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

FetchContent_Declare(
  libcuckoo
  GIT_REPOSITORY https://github.com/efficient/libcuckoo.git
  GIT_TAG        784d0f5d147b9a73f897ae55f6c3712d9a91b058
)

FetchContent_MakeAvailable(pybind11 gflags libcuckoo)
