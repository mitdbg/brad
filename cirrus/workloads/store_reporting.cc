#include <chrono>
#include <random>
#include <sstream>
#include <string>

#include "store.h"
#include "utils/connection.h"
#include "utils/dbtype.h"
#include "utils/sf.h"

SalesReporting::SalesReporting(uint32_t scale_factor, uint64_t num_warmup,
                               uint32_t client_id,
                               nanodbc::connection connection,
                               std::shared_ptr<BenchmarkState> state,
                               bool run_sim_etl)
    : WorkloadBase(std::move(state)),
      num_warmup_(num_warmup),
      num_reports_run_(0),
      scale_factor_(scale_factor),
      run_sim_etl_(run_sim_etl),
      connection_(std::move(connection)),
      prng_(42 ^ client_id) {
  Start();
}

void SalesReporting::RunImpl() {
  // Run this many reports per query, to amortize the cost of sending the query
  // over the network.
  // Originally this value was set to 10, but it seemed to be too intense.
  static constexpr uint32_t kRepetitions = 1;
  max_datetime_ = GetMaxDatetime();

  // NOTE: This is PostgreSQL-specific syntax. Postgres implements repeatable
  // read using snapshot isolation. We want this query to run over a
  // transactionally consistent snapshot of the data.
  nanodbc::execute(connection_,
                   "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL "
                   "REPEATABLE READ READ ONLY");

  for (uint64_t i = 0; i < num_warmup_; ++i) {
    nanodbc::execute(connection_, GenerateQuery(1));
  }

  WarmedUpAndReadyToRun();

  uint64_t num_iters = 0;
  while (KeepRunning()) {
    const auto start = std::chrono::steady_clock::now();
    GetState()->WaitIfETLInProgress();
    nanodbc::execute(connection_, GenerateQuery(kRepetitions));
    const auto end = std::chrono::steady_clock::now();
    AddLatency((end - start) / kRepetitions);
    num_reports_run_ += 10;

    if (!KeepRunning()) break;

    // Refresh the max datetime for the analytical queries.
    if (num_iters % 5 == 0) {
      max_datetime_ = GetMaxDatetime();
    }
  }
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

std::string SalesReporting::GenerateQuery(uint32_t repetitions) const {
  const auto [start, end] = GenerateDatetimeRange();
  const std::string psf = PaddedScaleFactor(scale_factor_);

  std::stringstream query;
  query << "SELECT i_id, i_category, SUM(s_price * s_quantity) AS volume ";
  query << "FROM sales_" << psf;
  query << ", inventory_" << psf;
  query << " WHERE s_datetime >= " << start;
  query << " AND s_datetime <= " << end;
  query << " AND i_id = s_i_id GROUP BY i_id, i_category; ";

  const std::string query_str = query.str();
  std::stringstream query_batch;
  for (uint32_t i = 0; i < repetitions; ++i) {
    query_batch << query_str;
  }

  return query_batch.str();
}

uint64_t SalesReporting::NumReportsRun() const { return num_reports_run_; }

uint64_t SalesReporting::GetMaxDatetime() const {
  auto result =
      nanodbc::execute(connection_, "SELECT MAX(s_datetime) FROM sales_" +
                                        PaddedScaleFactor(scale_factor_));
  result.next();
  return result.get<uint64_t>(0);
}
