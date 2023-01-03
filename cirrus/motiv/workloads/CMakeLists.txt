# Used to run the sales-inventory workload.
add_executable(run_store
  run_store.cc
  latency_manager.h
  workload_base.cc
  workload_base.h
  write_logger.cc
  write_logger.h
)
target_link_libraries(run_store PRIVATE cirrus gflags ycsbr)

# Used to generate the store dataset (for the sales-inventory workload).
add_executable(gen_store gen_store.cc)
target_link_libraries(gen_store PRIVATE cirrus gflags)

# Used to benchmark insert and IVM performance.
add_executable(bench_ivm bench_ivm.cc)
target_link_libraries(bench_ivm PRIVATE cirrus gflags ycsbr)

add_subdirectory(store)
