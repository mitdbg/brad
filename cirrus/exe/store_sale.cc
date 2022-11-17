#include <gflags/gflags.h>

#include <iostream>

#include "datasets/store.h"
#include "utils/connection.h"
#include "workloads/state.h"
#include "workloads/store.h"

// We map each client to a thread. We may want a different way to scale the
// workload.
DEFINE_uint32(clients, 1, "Number of clients used to make the requests");

DEFINE_uint32(sf, 1, "Dataset scale factor.");
DEFINE_uint64(warmup, 10, "Number of warm up iterations to run.");
DEFINE_uint32(run_for, 10, "How long to let the experiment run (in seconds).");

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Runs the 'sale' workload.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

  auto state = BenchmarkState::Create();
  std::vector<std::unique_ptr<MakeSale>> clients;

  std::cerr << "Dropping extraneous sales records..." << std::endl;
  {
    StoreDataset dataset(FLAGS_sf);
    nanodbc::connection c(utils::GetConnection());
    dataset.DropWorkloadGeneratedRecords(c);
  }

  std::cerr << "Starting up and warming up clients..." << std::endl;
  for (uint32_t i = 0; i < FLAGS_clients; ++i) {
    clients.push_back(std::make_unique<MakeSale>(FLAGS_sf, FLAGS_warmup,
                                                 /*client_id=*/i, state));
  }
  state->WaitUntilAllReady(/*expected=*/FLAGS_clients);

  const auto start = std::chrono::steady_clock::now();
  state->AllowStart();
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_run_for));
  state->SetStopRunning();
  for (auto& client : clients) {
    client->Wait();
  }
  const auto end = std::chrono::steady_clock::now();
  const auto elapsed = end - start;

  std::cerr << "> Ran for " << elapsed.count() << " ns" << std::endl;

  // Compute throughput.
  uint64_t total_sales = 0;
  for (auto& client : clients) {
    total_sales += client->NumTxnsRun();
  }
  const double thpt = total_sales / (elapsed.count() / 1e9);
  const double avg_latency = 1.0 / thpt;

  std::cerr << "> Throughput: " << thpt << " sales/s" << std::endl;
  std::cerr << "> Average latency: " << avg_latency << " s" << std::endl;

  std::cout << "thpt,avg_lat_s" << std::endl;
  std::cout << thpt << "," << avg_latency << std::endl;

  return 0;
}
