#pragma once

#include <nanodbc/nanodbc.h>

#include <cstdint>
#include <memory>
#include <random>
#include <thread>
#include <utility>

#include "state.h"
#include "workload_base.h"

// Runs the analytical query.
class SalesReporting {
 public:
  SalesReporting(uint64_t num_warmup, uint64_t max_datetime,
                 std::shared_ptr<BenchmarkState> state);
  ~SalesReporting();

  void Wait();
  uint64_t NumReportsRun() const { return num_reports_run_; }

 private:
  void Run();

  void RunBaseReporting();
  void RunEagerReporting();
  void RunDeferredReporting();
  void RunPartitionedReporting();

  std::pair<uint64_t, uint64_t> GenerateDatetimeRange();

  uint64_t num_warmup_;
  uint64_t max_datetime_;
  uint64_t num_reports_run_;

  nanodbc::connection connection_;
  std::mt19937 prng_;
  bool joined_;
  std::shared_ptr<BenchmarkState> state_;
  std::thread thread_;
};

class MakeSale : public WorkloadBase {
 public:
  MakeSale(uint64_t num_warmup, uint32_t seed, std::shared_ptr<BenchmarkState> state);
  virtual ~MakeSale() = default;

  uint64_t NumTxnsRun() const;

 private:
  virtual void RunImpl() override;
  uint64_t GetMaxItemId();

  uint64_t num_warmup_;
  uint64_t num_txns_;
  uint32_t seed_;
  nanodbc::connection connection_;
};
