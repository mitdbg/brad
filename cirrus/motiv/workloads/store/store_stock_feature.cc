#include "store.h"

#include "cirrus/stats.h"

namespace cirrus {

StockFeature::StockFeature(StockFeatureOptions options,
                           std::shared_ptr<Cirrus> cirrus,
                           std::shared_ptr<BenchmarkState> state)
    : WorkloadBase(std::move(state)),
      num_queries_(0),
      options_(options),
      cirrus_(std::move(cirrus)) {
  Start();
}

uint64_t StockFeature::NumQueries() const { return num_queries_; }

void StockFeature::RunImpl() {
  cirrus_->EstablishThreadLocalConnections();

  for (uint64_t i = 0; i < options_.num_warmup; ++i) {
    cirrus_->RunStockFeatureQuery();
  }

  WarmedUpAndReadyToRun();

  while (KeepRunning()) {
    const auto start = std::chrono::steady_clock::now();
    cirrus_->RunStockFeatureQuery();
    const auto end = std::chrono::steady_clock::now();
    AddLatency(end - start);
    ++num_queries_;
  }

  Stats::Local().PostToGlobal();
}

}  // namespace cirrus
