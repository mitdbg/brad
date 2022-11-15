#include <gflags/gflags.h>
#include <nanodbc/nanodbc.h>

#include <chrono>
#include <iostream>
#include <optional>
#include <sstream>
#include <unordered_map>

#include "utils/connection.h"
#include "utils/dbtype.h"
#include "datasets/store.h"
#include "workloads/state.h"
#include "workloads/store.h"

DEFINE_bool(drop_all, false, "Set this flag to drop all state.");
DEFINE_bool(load, false, "Set this flag to load the dataset.");
DEFINE_string(exp, "", "The experiment to run.");

DEFINE_uint32(sf, 0, "Specifies the dataset scale factor.");
DEFINE_double(read_latest_prob, 0.05,
              "The chance that the reporting query reads the latest data");

DEFINE_uint64(warmup, 10, "Number of warm up iterations to run.");
DEFINE_uint32(run_for, 10, "How long to let the experiment run (in seconds).");

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Run data orchestration experiments using ODBC.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

  auto const connstr = NANODBC_TEXT(Connection::GetConnectionString(DBType::kRedshift));
  nanodbc::connection c(connstr);

  StoreDataset store(FLAGS_sf);

  if (FLAGS_sf == 0) {
    std::cerr << "ERROR: Please set the scale factor --sf." << std::endl;
    return 1;
  }

  if (FLAGS_load) {
    // Set up the database for experiments.
    store.DropAll(c);
    store.CreateTables(c);
    store.GenerateAndLoad(c);

  } else if (FLAGS_drop_all) {
    store.DropAll(c);
  }

  if (!FLAGS_exp.empty()) {
    const uint64_t max_datetime = store.GetMaxDatetime(c);
    auto state = BenchmarkState::Create();

    std::cerr << "Warming up reader..." << std::endl;
    std::unique_ptr<SalesReporting> reader = std::make_unique<SalesReporting>(
        FLAGS_warmup, max_datetime, state);
    state->SpinWaitUntilAllReady(/*expected=*/1);

    const auto start = std::chrono::steady_clock::now();
    state->AllowStart();
    std::this_thread::sleep_for(std::chrono::seconds(FLAGS_run_for));
    state->SetStopRunning();
    reader->Wait();
    const auto read_end = std::chrono::steady_clock::now();
    const auto read_elapsed = read_end - start;

    std::cerr << "> Reader ran for " << read_elapsed.count() << " ns"
              << std::endl;

    // Compute throughput.
    const double read_thpt =
        reader->NumReportsRun() / (read_elapsed.count() / 1e9);
    const double avg_read_latency = 1.0 / read_thpt;

    std::cerr << "> Read throughput: " << read_thpt << " reports/s"
              << std::endl;
    std::cerr << "> Read latency: " << avg_read_latency << " s"
              << std::endl;

    std::cout << "read_thpt,read_lat_s" << std::endl;
    std::cout << read_thpt << "," << avg_read_latency << std::endl;

  }

  return 0;
}
