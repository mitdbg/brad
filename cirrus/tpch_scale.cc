#include <gflags/gflags.h>
#include <nanodbc/nanodbc.h>

#include <chrono>
#include <iostream>
#include <optional>
#include <sstream>
#include <unordered_map>

#include "utils/connection.h"
#include "workloads/state.h"
#include "workloads/tpch.h"

DEFINE_uint32(sf, 0, "Specifies the dataset scale factor.");
DEFINE_uint32(batch_size, 10, "Set the batch size.");
DEFINE_uint64(warmup, 10, "Number of warm up iterations to run.");
DEFINE_uint32(run_for, 10, "How long to let the experiment run (in seconds).");

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Run TPC-H experiments using ODBC.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

  if (FLAGS_sf == 0) {
    std::cerr << "ERROR: Please set the scale factor --sf." << std::endl;
    return 1;
  }

  Connection::InitConnectionString();
  auto const connstr = NANODBC_TEXT(Connection::GetConnectionString());
  nanodbc::connection c(connstr);

  auto state = BenchmarkState::Create();

  std::cerr << "> Warming up reader..." << std::endl;
  std::unique_ptr<RunQ5> reader =
      std::make_unique<RunQ5>(FLAGS_warmup, FLAGS_batch_size, FLAGS_sf, state);
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

  // Compute throughput and latency.
  const double read_thpt =
      reader->NumQueriesRun() / (read_elapsed.count() / 1e9);
  const double avg_read_latency = 1.0 / read_thpt;

  std::cerr << "> Read throughput: " << read_thpt << " reports/s" << std::endl;
  std::cerr << "> Read latency: " << avg_read_latency << " s" << std::endl;

  std::cout << "sf,read_thpt,read_lat_s" << std::endl;
  std::cout << FLAGS_sf << "," << read_thpt << "," << avg_read_latency
            << std::endl;

  return 0;
}
