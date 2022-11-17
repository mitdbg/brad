#include <gflags/gflags.h>

#include <iostream>

#include "datasets/store.h"
#include "utils/connection.h"
#include "workloads/state.h"
#include "workloads/store.h"

// We map each client to a thread. We may want a different way to scale the
// workload.
DEFINE_uint32(tclients, 0, "Number of clients used to make the write requests");
DEFINE_uint32(aclients, 0,
              "Number of clients used to make the analytical requests");

DEFINE_uint32(sf, 1, "Dataset scale factor.");
DEFINE_uint64(warmup, 10, "Number of warm up iterations to run.");
DEFINE_uint32(run_for, 10, "How long to let the experiment run (in seconds).");

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Runs the 'sale' workload.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

  if ((FLAGS_tclients + FLAGS_aclients) == 0) {
    std::cerr << "ERROR: Need to have at least one client." << std::endl;
    return 1;
  }

  auto state = BenchmarkState::Create();
  std::vector<std::unique_ptr<MakeSale>> tclients;
  std::vector<std::unique_ptr<SalesReporting>> aclients;

  std::cerr << "> Dropping extraneous sales records..." << std::endl;
  {
    StoreDataset dataset(FLAGS_sf);
    nanodbc::connection c(utils::GetConnection());
    dataset.DropWorkloadGeneratedRecords(c);
  }

  std::cerr << "> Starting up and warming up aclients..." << std::endl;
  for (uint32_t i = 0; i < FLAGS_aclients; ++i) {
    aclients.push_back(std::make_unique<SalesReporting>(FLAGS_sf, FLAGS_warmup,
                                                        /*client_id=*/i,
                                                        state));
  }
  state->WaitUntilAllReady(/*expected=*/FLAGS_aclients);

  std::cerr << "> Starting up and warming up tclients..." << std::endl;
  for (uint32_t i = 0; i < FLAGS_tclients; ++i) {
    tclients.push_back(std::make_unique<MakeSale>(FLAGS_sf, FLAGS_warmup,
                                                  /*client_id=*/i, state));
  }
  state->WaitUntilAllReady(/*expected=*/FLAGS_tclients + FLAGS_aclients);
  std::cerr << "> Warm up done. Starting the workload." << std::endl;

  const auto start = std::chrono::steady_clock::now();
  state->AllowStart();
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_run_for));
  state->SetStopRunning();
  for (auto& client : tclients) {
    client->Wait();
  }
  const auto write_end = std::chrono::steady_clock::now();
  for (auto& client : aclients) {
    client->Wait();
  }
  const auto read_end = std::chrono::steady_clock::now();
  const auto write_elapsed = write_end - start;
  const auto read_elapsed = read_end - start;

  std::cerr << "> T clients ran for " << write_elapsed.count() << " ns"
            << std::endl;
  std::cerr << "> A clients ran for " << read_elapsed.count() << " ns"
            << std::endl;

  // Compute throughput.
  uint64_t total_sales = 0;
  uint64_t aborts = 0;
  uint64_t reports = 0;
  for (auto& client : tclients) {
    total_sales += client->NumTxnsRun();
    aborts += client->NumAborts();
  }
  for (auto& client : aclients) {
    reports += client->NumReportsRun();
  }
  const double t_thpt = total_sales / (write_elapsed.count() / 1e9);
  const double avg_abort_rate =
      static_cast<double>(aborts) / (aborts + total_sales);
  const double a_thpt = reports / (read_elapsed.count() / 1e9);

  std::cerr << "> T Throughput: " << t_thpt << " sales/s" << std::endl;
  std::cerr << "> A Throughput: " << a_thpt << " reports/s" << std::endl;
  std::cerr << "> Average abort rate: " << avg_abort_rate << std::endl;

  std::cout << "t_thpt,avg_abort_rate,a_thpt" << std::endl;
  std::cout << t_thpt << "," << avg_abort_rate << "," << a_thpt << std::endl;

  return 0;
}
