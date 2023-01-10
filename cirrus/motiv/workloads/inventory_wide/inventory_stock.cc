#include <chrono>
#include <random>
#include <sstream>
#include <string>

#include "cirrus/stats.h"
#include "inventory_wide.h"

namespace cirrus {

CategoryStock::CategoryStock(CategoryStockOptions options,
                               std::shared_ptr<Cirrus> cirrus,
                               std::shared_ptr<BenchmarkState> state)
    : WorkloadBase(std::move(state)),
      options_(options),
      num_reports_run_(0),
      cirrus_(std::move(cirrus)),
      prng_(42 ^ options_.client_id) {
  Start();
}

void CategoryStock::RunImpl() {
  cirrus_->EstablishThreadLocalConnections();

  for (uint64_t i = 0; i < options_.num_warmup; ++i) {
    cirrus_->RunCategoryStockQuery();
  }

  WarmedUpAndReadyToRun();

  uint64_t num_iters = 0;
  while (KeepRunning()) {
    const auto start = std::chrono::steady_clock::now();
    cirrus_->RunCategoryStockQuery();
    const auto end = std::chrono::steady_clock::now();
    AddLatency(end - start);
    ++num_reports_run_;
  }

  Stats::Local().PostToGlobal();
}

uint64_t CategoryStock::NumReportsRun() const { return num_reports_run_; }

}  // namespace cirrus
