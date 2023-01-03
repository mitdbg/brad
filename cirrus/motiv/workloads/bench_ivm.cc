#include <gflags/gflags.h>

#include <chrono>
#include <fstream>
#include <iostream>
#include <random>

#include "cirrus/cirrus.h"
#include "cirrus/config.h"
#include "cirrus/odbc.h"
#include "cirrus/stats.h"
#include "datasets/store.h"
#include "workloads/store/store.h"
#include "write_logger.h"

using namespace cirrus;

// Can also set these flags instead of `--config_file`.
DEFINE_string(dsn, "", "ODBC DSN for local connections.");
DEFINE_string(user, "", "ODBC username for local connections.");
DEFINE_string(pwdvar, "", "ODBC password variable for local connections.");

DEFINE_uint32(sf, 0, "Specifies the dataset scale factor.");
DEFINE_uint64(warmup, 10, "Number of warm up iterations to run.");
DEFINE_uint32(run_for, 10, "How long to let the experiment run (in seconds).");

DEFINE_bool(run_ivm, false, "If set, will refresh the materialized view too.");

namespace {

static const std::string kInsertSales =
    "INSERT INTO sales (s_id, s_datetime, s_i_id, s_quantity, s_price, "
    "s_phys_id) VALUES (?, ?, ?, ?, ?, ?)";

static const std::string kGetStats =
    "SELECT MAX(s_id), MAX(s_datetime), MAX(s_i_id) FROM sales";

void RunExperiment() {}

}  // namespace

int main(int argc, char* argv[]) {
  // Hypothesis is that Redshift's insert and/or IVM performance is very poor.
  gflags::SetUsageMessage(
      "Used to benchmark Redshift insert and IVM performance.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

  if (FLAGS_sf == 0) {
    std::cerr << "ERROR: Please set the scale factor --sf." << std::endl;
    return 1;
  }

  StoreDataset store(FLAGS_sf);
  auto config = CirrusConfig::Local(FLAGS_dsn, FLAGS_user, FLAGS_pwdvar);

  std::cerr << "> Dropping extraneous sales records and retrieving stats..."
            << std::endl;
  uint64_t max_id, max_datetime, max_item_id;
  nanodbc::connection c = GetOdbcConnection(*config, config->read_store_type());
  {
    store.DropWorkloadGeneratedRecords(c);
    if (FLAGS_run_ivm) {
      nanodbc::execute(c, "REFRESH MATERIALIZED VIEW volumes");
    }
    auto res = nanodbc::execute(c, kGetStats);
    res.next();
    max_id = res.get<uint64_t>(0);
    max_datetime = res.get<uint64_t>(1);
    max_item_id = res.get<uint64_t>(2);
  }

  std::mt19937 prng(42);
  const uint64_t max_datetime_gap = 10ULL;
  std::uniform_int_distribution<uint64_t> datetime_gap_dist(1,
                                                            max_datetime_gap);

  const size_t kNumLatencyMeasurements = 100'000;
  std::vector<std::chrono::nanoseconds> latency;
  latency.reserve(kNumLatencyMeasurements);
  size_t next_lat_idx = 0;

  // We fix these values.
  const uint64_t quantity = 1000;
  const uint64_t item_id = max_item_id;
  const uint64_t price = 101;

  nanodbc::statement stmt(c);

  std::cerr << "> Starting experiment..." << std::endl;
  uint64_t num_trials = 0;
  const auto overall_start = std::chrono::steady_clock::now();
  const auto run_until = overall_start + std::chrono::seconds(FLAGS_run_for);
  while (true) {
    const uint64_t next_datetime = max_datetime + datetime_gap_dist(prng);
    const uint64_t next_id = max_id + 1;
    const auto trial_start = std::chrono::steady_clock::now();
    nanodbc::prepare(stmt, kInsertSales);
    stmt.bind(0, &next_id, 1);
    stmt.bind(1, &next_datetime, 1);
    stmt.bind(2, &item_id, 1);
    stmt.bind(3, &quantity, 1);
    stmt.bind(4, &price, 1);
    // `s_phys_id`
    stmt.bind(5, &next_id, 1);
    nanodbc::execute(stmt);
    if (FLAGS_run_ivm) {
      nanodbc::prepare(stmt, "REFRESH MATERIALIZED VIEW volumes");
      nanodbc::execute(stmt);
    }
    const auto trial_end = std::chrono::steady_clock::now();
    const auto lat = trial_end - trial_start;
    if (latency.size() < kNumLatencyMeasurements) {
      latency.push_back(lat);
    } else {
      latency[next_lat_idx++] = lat;
      if (next_lat_idx >= kNumLatencyMeasurements) {
        next_lat_idx = 0;
      }
    }
    max_datetime = next_datetime;
    max_id = next_id;
    ++num_trials;

    if (trial_end >= run_until) {
      break;
    }
  }
  const auto overall_end = std::chrono::steady_clock::now();
  const auto overall_elapsed = overall_end - overall_start;

  // Report results.
  std::cerr << "> Ran for " << overall_elapsed.count() << " ns" << std::endl;
  std::cerr << "> Trials: " << num_trials << std::endl;

  std::sort(latency.begin(), latency.end());
  const double ins_per_s = num_trials / (overall_elapsed.count() / 1e9);

  const auto p50_lat_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      latency.at(latency.size() / 2));
  const auto p99_lat_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      latency.at(latency.size() * 0.99));

  std::cerr << "> Throughput: " << ins_per_s << " inserts/s" << std::endl;
  std::cerr << "> p50 Latency: " << p50_lat_ms.count() << " ms" << std::endl;
  std::cerr << "> p99 Latency: " << p99_lat_ms.count() << " ms" << std::endl;
  std::cerr << std::endl;

  std::cout << "ins_per_s,p50_ms,p99_ms" << std::endl;
  std::cout << ins_per_s << "," << p50_lat_ms.count() << ","
            << p99_lat_ms.count() << std::endl;

  return 0;
}
