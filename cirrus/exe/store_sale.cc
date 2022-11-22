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

DEFINE_string(
    read_db, "rdspg",
    "Which system to use for the analytical queries {rdspg, redshift}.");
DEFINE_uint32(etl_period_ms, 10000, "How often to run the ETL.");

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Runs the 'sale' workload.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

  if ((FLAGS_tclients + FLAGS_aclients) == 0) {
    std::cerr << "ERROR: Need to have at least one client." << std::endl;
    return 1;
  }

  auto maybe_read_db = dbtype::FromString(FLAGS_read_db);
  if (!maybe_read_db.has_value()) {
    std::cerr << "ERROR: Unrecognized DB " << FLAGS_read_db << std::endl;
    return 1;
  }
  const DBType read_db = *maybe_read_db;

  auto state = BenchmarkState::Create();

  std::vector<std::unique_ptr<MakeSale>> tclients;
  std::vector<std::unique_ptr<SalesReporting>> aclients;
  std::unique_ptr<SalesETL> etl;

  std::cerr << "> Dropping extraneous sales records..." << std::endl;
  {
    StoreDataset dataset(FLAGS_sf);
    {
      nanodbc::connection c(utils::GetConnection(DBType::kRDSPostgreSQL));
      dataset.DropWorkloadGeneratedRecords(c);
    }
    if (read_db == DBType::kRedshift) {
      nanodbc::connection c(utils::GetConnection(DBType::kRedshift));
      dataset.DropWorkloadGeneratedRecords(c);
    }
  }

  std::cerr << "> Starting up and warming up aclients..." << std::endl;
  for (uint32_t i = 0; i < FLAGS_aclients; ++i) {
    aclients.push_back(std::make_unique<SalesReporting>(
        FLAGS_sf, FLAGS_warmup,
        /*client_id=*/i, utils::GetConnection(read_db), state,
        /*run_sim_etl=*/read_db == DBType::kRedshift));
  }
  state->WaitUntilAllReady(/*expected=*/FLAGS_aclients);

  std::cerr << "> Starting up and warming up tclients..." << std::endl;
  for (uint32_t i = 0; i < FLAGS_tclients; ++i) {
    tclients.push_back(std::make_unique<MakeSale>(
        FLAGS_sf, FLAGS_warmup,
        /*client_id=*/i, utils::GetConnection(DBType::kRDSPostgreSQL), state));
  }
  state->WaitUntilAllReady(/*expected=*/FLAGS_tclients + FLAGS_aclients);

  if (read_db == DBType::kRedshift) {
    std::cerr << "> Starting up the ETL orchestrator..." << std::endl;
    etl = std::make_unique<SalesETL>(
        FLAGS_sf, std::chrono::milliseconds(FLAGS_etl_period_ms),
        /*source=*/utils::GetConnection(DBType::kRDSPostgreSQL),
        /*dest=*/utils::GetConnection(DBType::kRedshift), state);
    state->WaitUntilAllReady(/*expected=*/FLAGS_tclients + FLAGS_aclients + 1);
  }
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
  if (etl != nullptr) {
    etl->Wait();
  }
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
    client->SortLatency();
  }
  for (auto& client : aclients) {
    reports += client->NumReportsRun();
    client->SortLatency();
  }
  const double t_thpt = total_sales / (write_elapsed.count() / 1e9);
  const double avg_abort_rate =
      static_cast<double>(aborts) / (aborts + total_sales);
  const double a_thpt = reports / (read_elapsed.count() / 1e9);

  // TODO: Should consider latency across all clients.
  const auto t_lat_p50_ms =
      tclients.empty() ? 0 : tclients.front()->LatencyP50().count();
  const auto t_lat_p99_ms =
      tclients.empty() ? 0 : tclients.front()->LatencyP99().count();
  const auto a_lat_p50_ms =
      aclients.empty() ? 0 : aclients.front()->LatencyP50().count();
  const auto a_lat_p99_ms =
      aclients.empty() ? 0 : aclients.front()->LatencyP99().count();

  std::cerr << std::endl;
  std::cerr << "> T Throughput: " << t_thpt << " sales/s" << std::endl;
  std::cerr << "> A Throughput: " << a_thpt << " reports/s" << std::endl;
  std::cerr << "> Average abort rate: " << avg_abort_rate << std::endl;
  std::cerr << std::endl;
  std::cerr << "> T p50 Latency: " << t_lat_p50_ms << " ms" << std::endl;
  std::cerr << "> T p99 Latency: " << t_lat_p99_ms << " ms" << std::endl;
  std::cerr << "> A p50 Latency: " << a_lat_p50_ms << " ms" << std::endl;
  std::cerr << "> A p99 Latency: " << a_lat_p99_ms << " ms" << std::endl;
  std::cerr << std::endl;

  if (etl != nullptr) {
    etl->SortLatency();
    std::cerr << "> ETL runs: " << etl->NumRuns() << std::endl;
    std::cerr << "> ETL p50 Latency: " << etl->LatencyP50().count() << " ms"
              << std::endl;
    std::cerr << "> ETL p99 Latency: " << etl->LatencyP50().count() << " ms"
              << std::endl;
    std::cerr << std::endl;
  }

  std::cout << "t_thpt,avg_abort_rate,a_thpt,t_p50_ms,t_p99_ms,a_p50_ms,a_p99_"
               "ms,etl_runs,etl_p50_ms,etl_p99_ms"
            << std::endl;
  std::cout << t_thpt << "," << avg_abort_rate << "," << a_thpt << ","
            << t_lat_p50_ms << "," << t_lat_p99_ms << "," << a_lat_p50_ms << ","
            << a_lat_p99_ms << ",";
  if (etl != nullptr) {
    std::cout << etl->NumRuns() << "," << etl->LatencyP50().count() << ","
              << etl->LatencyP99().count();
  } else {
    std::cout << "0,0,0";
  }
  std::cout << std::endl;

  return 0;
}
