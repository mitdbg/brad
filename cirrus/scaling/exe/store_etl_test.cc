#include <gflags/gflags.h>

#include <iostream>

#include "datasets/store.h"
#include "utils/connection.h"
#include "workloads/state.h"
#include "workloads/store.h"

DEFINE_uint32(sf, 1, "Dataset scale factor.");
DEFINE_uint32(run_for, 10, "How long to let the experiment run (in seconds).");
DEFINE_uint32(etl_period_ms, 10000, "How often to run the ETL.");

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Runs the 'sale' workload.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

  auto state = BenchmarkState::Create();

  std::cerr << "> Starting up the ETL orchestrator..." << std::endl;
  std::unique_ptr<SalesETL> etl = std::make_unique<SalesETL>(
      FLAGS_sf, std::chrono::milliseconds(FLAGS_etl_period_ms),
      /*source=*/utils::GetConnection(DBType::kRDSPostgreSQL),
      /*dest=*/utils::GetConnection(DBType::kRedshift), state);
  state->WaitUntilAllReady(/*expected=*/1);

  std::cerr << "> Warm up done. Starting the workload." << std::endl;

  const auto start = std::chrono::steady_clock::now();
  state->AllowStart();
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_run_for));
  state->SetStopRunning();
  etl->Wait();
  const auto elapsed = std::chrono::steady_clock::now() - start;

  etl->SortLatency();
  std::cerr << "> ETL runs: " << etl->NumRuns() << std::endl;
  std::cerr << "> ETL p50 Latency: " << etl->LatencyP50().count() << " ms"
            << std::endl;
  std::cerr << "> ETL p99 Latency: " << etl->LatencyP50().count() << " ms"
            << std::endl;

  return 0;
}
