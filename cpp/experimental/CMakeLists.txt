include(FetchContent)
include(ExternalProject)

FetchContent_Declare(
  gRPC_src
  GIT_REPOSITORY https://github.com/grpc/grpc
  GIT_TAG        v1.64.2
)
FetchContent_Populate(gRPC_src)

ExternalProject_Add(
  gRPC_src2
  SOURCE_DIR ${CMAKE_BINARY_DIR}/_deps/grpc_src-src
  INSTALL_COMMAND ""
)
add_library(gRPC STATIC IMPORTED GLOBAL)
add_dependencies(gRPC gRPC_src2)
set_target_properties(
  gRPC PROPERTIES IMPORTED_LOCATION
  ${CMAKE_CURRENT_BINARY_DIR}/gRPC_src2-prefix/src/gRPC_src2-build/libgrpc++.a)
target_include_directories(gRPC INTERFACE
  ${CMAKE_BINARY_DIR}/_deps/grpc_src-src/include)

add_executable(proxy_socket proxy_socket.cc)
target_link_libraries(proxy_socket PRIVATE gflags)

add_executable(brad_grpc brad_grpc.cc brad.grpc.pb.cc brad.grpc.pb.h brad.pb.cc)
target_link_libraries(brad_grpc PRIVATE gRPC)
