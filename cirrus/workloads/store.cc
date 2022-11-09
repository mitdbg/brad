#include "store.h"

#include <random>
#include <string>
#include <iostream>

#include "../connection.h"

SalesReporting::SalesReporting(uint64_t num_warmup, uint64_t max_datetime,
                               std::shared_ptr<BenchmarkState> state)
    : num_warmup_(num_warmup),
      max_datetime_(max_datetime),
      num_reports_run_(0),
      connection_(Connection::GetConnectionString()),
      prng_(42),
      joined_(false),
      state_(std::move(state)) {
  thread_ = std::thread(&SalesReporting::Run, this);
}

SalesReporting::~SalesReporting() { Wait(); }

void SalesReporting::Wait() {
  if (joined_) return;
  thread_.join();
  joined_ = true;
}

void SalesReporting::Run() { RunBaseReporting(); }

void SalesReporting::RunBaseReporting() {
  // Prepare the query.
  // We run the same fixed query.
  // TODO: These prepared statements do not provide much benefit when using
  // ODBC. There is less overhead if we send over a string with the same query
  // included multiple times.
  nanodbc::statement stmt(connection_);
  stmt.prepare(
      "SELECT i_id, i_category, SUM(s_price * s_quantity) AS volume "
      "FROM sales, inventory "
      "WHERE s_datetime >= ? AND s_datetime <= ? "
      "AND i_id = s_i_id "
      "GROUP BY i_id, i_category;");
  const auto [start, end] = GenerateDatetimeRange();
  stmt.bind(0, &start, 1);
  stmt.bind(1, &end, 1);

  const auto run_txn = [&]() {
    stmt.execute();
  };

  for (uint64_t i = 0; i < num_warmup_; ++i) {
    run_txn();
  }

  state_->BumpReady();
  state_->WaitToStart();

  while (state_->KeepRunning()) {
    run_txn();
    ++num_reports_run_;
  }
}

std::pair<uint64_t, uint64_t> SalesReporting::GenerateDatetimeRange() {
  // The datetime range usually starts in the first quarter.
  std::normal_distribution<double> start_dist(max_datetime_ / 4.0,
                                              /*stddev=*/2.0);
  // The length of a scan is usually a fifth of the dataset, but with wide
  // tails.
  std::normal_distribution<double> length_dist(max_datetime_ / 5.0,
                                               /*stddev=*/4.0);

  std::uniform_real_distribution<double> read_recent(0, 1.0);

  const double length_dbl = length_dist(prng_);
  const uint64_t length =
      length_dbl < 0 ? 1 : static_cast<uint64_t>(length_dbl);

  const double start_dbl = start_dist(prng_);
  const uint64_t start = start_dbl < 0 ? 0 : static_cast<uint64_t>(start_dbl);
  return {start, std::min(max_datetime_, start + length)};
}
