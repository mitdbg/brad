#pragma once

#include <nanodbc/nanodbc.h>

#include <memory>
#include <thread>

#include "state.h"

class RunQ5 {
 public:
  RunQ5(uint64_t num_warmup, uint64_t batch_size, uint32_t scale_factor,
        std::shared_ptr<BenchmarkState> state);
  ~RunQ5();

  void Wait();
  uint64_t NumQueriesRun() const { return num_queries_run_; }

 private:
  void Run();

  uint64_t num_warmup_;
  uint64_t batch_size_;
  uint32_t scale_factor_;
  uint64_t num_queries_run_;
  std::shared_ptr<BenchmarkState> state_;
  nanodbc::connection connection_;
  bool joined_;
  std::thread thread_;
};
