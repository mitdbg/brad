#pragma once

#include <nanodbc/nanodbc.h>

#include <memory>
#include <string>
#include <thread>

#include "state.h"
#include "../utils/dbtype.h"

class RunQuery {
 public:
  RunQuery(uint64_t num_warmup, uint64_t batch_size, std::string query,
           std::shared_ptr<BenchmarkState> state, DBType dbtype);
  ~RunQuery();

  void Wait();
  uint64_t NumQueriesRun() const { return num_queries_run_; }

 private:
  void Run();

  std::string query_;
  uint64_t num_warmup_;
  uint64_t batch_size_;
  uint64_t num_queries_run_;
  std::shared_ptr<BenchmarkState> state_;
  nanodbc::connection connection_;
  bool joined_;
  std::thread thread_;
};

namespace tpch {

std::string Query5(uint32_t sf);
std::string Query3(uint32_t sf);

}  // namespace tpch
