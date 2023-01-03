#include <chrono>
#include <random>
#include <sstream>
#include <string>

#include "cirrus/stats.h"
#include "store.h"

namespace cirrus {

SalesReporting::SalesReporting(SalesReportingOptions options,
                               std::shared_ptr<Cirrus> cirrus,
                               std::shared_ptr<BenchmarkState> state)
    : WorkloadBase(std::move(state)),
      options_(options),
      max_datetime_(0),
      num_reports_run_(0),
      cirrus_(std::move(cirrus)),
      prng_(42 ^ options_.client_id) {
  Start();
}

void SalesReporting::RunImpl() {
  cirrus_->EstablishThreadLocalConnections();
  max_datetime_ = cirrus_->GetMaxDatetime();

  for (uint64_t i = 0; i < options_.num_warmup; ++i) {
    const auto [start, end] = GenerateDatetimeRange();
    cirrus_->RunReportingQuery(start, end);
  }

  WarmedUpAndReadyToRun();

  uint64_t num_iters = 0;
  while (KeepRunning()) {
    const auto start = std::chrono::steady_clock::now();
    const auto [datetime_start, datetime_end] = GenerateDatetimeRange();
    cirrus_->RunReportingQuery(datetime_start, datetime_end);
    const auto end = std::chrono::steady_clock::now();
    AddLatency(end - start);
    ++num_reports_run_;

    if (!KeepRunning()) break;

    // Refresh the max datetime for the analytical queries.
    if (num_iters % 5 == 0) {
      max_datetime_ = cirrus_->GetMaxDatetime();
    }
  }

  Stats::Local().PostToGlobal();
}

std::pair<uint64_t, uint64_t> SalesReporting::GenerateDatetimeRange() const {
  // The datetime range usually starts in the first quarter.
  std::normal_distribution<double> start_dist(max_datetime_ / 4.0,
                                              /*stddev=*/2.0);
  // The length of a scan is usually 1/20 of the dataset, but with wide tails.
  std::normal_distribution<double> length_dist(max_datetime_ / 20.0,
                                               /*stddev=*/4.0);

  std::uniform_real_distribution<double> read_recent(0, 1.0);

  const double length_dbl = length_dist(prng_);
  const uint64_t length =
      length_dbl < 0 ? 1 : static_cast<uint64_t>(length_dbl);

  const double start_dbl = start_dist(prng_);
  const uint64_t start = start_dbl < 0 ? 0 : static_cast<uint64_t>(start_dbl);
  return {start, std::min(max_datetime_, start + length)};
}

uint64_t SalesReporting::NumReportsRun() const { return num_reports_run_; }

}  // namespace cirrus
