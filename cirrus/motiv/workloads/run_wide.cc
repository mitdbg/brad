#include <gflags/gflags.h>

#include <fstream>
#include <iostream>

#include "cirrus/cirrus.h"
#include "cirrus/config.h"
#include "cirrus/odbc.h"
#include "cirrus/stats.h"
#include "dataset_admin/dataset_admin.h"
#include "workloads/inventory_wide/inventory_wide.h"
#include "write_logger.h"

using namespace cirrus;

DEFINE_string(config_file, "", "Path to the Cirrus configuration file.");
DEFINE_string(dataset_config_file, "",
              "Path to the dataset configuration file.");

// Can also set these flags instead of `--config_file`.
DEFINE_string(dsn, "", "ODBC DSN for local connections.");
DEFINE_string(user, "", "ODBC username for local connections.");
DEFINE_string(pwdvar, "", "ODBC password variable for local connections.");

DEFINE_uint32(sf, 0, "Specifies the dataset scale factor.");
DEFINE_uint64(warmup, 10, "Number of warm up iterations to run.");
DEFINE_uint32(run_for, 10, "How long to let the experiment run (in seconds).");

DEFINE_double(theta, 0.9, "Workload skew factor.");

// We map each client to a thread. We may want a different way to scale the
// workload.
DEFINE_uint32(tclients, 0, "Number of clients used to make the write requests");
DEFINE_uint32(aclients, 0,
              "Number of clients used to make the analytical requests");

DEFINE_string(strategy, "wide_write", "The experiment strategy to use.");

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Runs workloads on the sales-inventory dataset.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

  if (FLAGS_sf == 0) {
    std::cerr << "ERROR: Please set the scale factor --sf." << std::endl;
    return 1;
  }

  auto config = !FLAGS_config_file.empty()
                    ? CirrusConfig::LoadFrom(FLAGS_config_file)
                    : CirrusConfig::Local(FLAGS_dsn, FLAGS_user, FLAGS_pwdvar);

  auto dataset = DatasetAdmin(FLAGS_dataset_config_file, FLAGS_sf);

  std::cerr << "> Dropping extraneous records..." << std::endl;
  {
    nanodbc::connection c =
        GetOdbcConnection(*config, config->write_store_type());
    dataset.ResetToGenerated(c, config->write_store_type());
  }
  if (config->read_store_type() != config->write_store_type()) {
    nanodbc::connection c =
        GetOdbcConnection(*config, config->read_store_type());
    dataset.ResetToGenerated(c, config->read_store_type());
  }

  const Strategy strategy = StrategyFromString(FLAGS_strategy);

  auto state = BenchmarkState::Create();
  std::shared_ptr<Cirrus> cirrus =
      std::shared_ptr<Cirrus>(Cirrus::Open(config, strategy));

  cirrus->EstablishThreadLocalConnections();

  std::vector<std::unique_ptr<InvMakeSale>> tclients;
  std::vector<std::unique_ptr<CategoryStock>> aclients;

  // Start up the clients.
  std::cerr << "> Starting up and warming up aclients..." << std::endl;
  CategoryStockOptions aoptions;
  aoptions.num_warmup = FLAGS_warmup;
  aoptions.scale_factor = FLAGS_sf;
  for (uint32_t i = 0; i < FLAGS_aclients; ++i) {
    aoptions.client_id = i;
    aclients.push_back(std::make_unique<CategoryStock>(aoptions, cirrus, state));
  }
  state->WaitUntilAllReady(/*expected=*/FLAGS_aclients);

  std::cerr << "> Starting up and warming up tclients..." << std::endl;
  MakeSaleOptions toptions;
  toptions.scale_factor = FLAGS_sf;
  toptions.num_warmup = FLAGS_warmup;
  toptions.theta = FLAGS_theta;
  // TODO: Number of items
  toptions.max_i_id = 10000000;
  for (uint32_t i = 0; i < FLAGS_tclients; ++i) {
    toptions.client_id = i;
    tclients.push_back(std::make_unique<InvMakeSale>(
        toptions,
        /*connection=*/GetOdbcConnection(*config, DBType::kRDSPostgreSQL),
        cirrus, state));
  }
  state->WaitUntilAllReady(/*expected=*/FLAGS_aclients + FLAGS_tclients);
  std::cerr << "> Warm up done. Starting the workload." << std::endl;

  // Run the workload.
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

  // Report results.
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

  std::cout
      << "t_thpt,avg_abort_rate,a_thpt,t_p50_ms,t_p99_ms,a_p50_ms,a_p99_ms"
      << std::endl;
  std::cout << t_thpt << "," << avg_abort_rate << "," << a_thpt << ","
            << t_lat_p50_ms << "," << t_lat_p99_ms << "," << a_lat_p50_ms << ","
            << a_lat_p99_ms << std::endl;

  // Make sure all background workers finish before writing out the stats.
  std::cerr << std::endl;
  std::cerr << "> Waiting for background workers to finish..." << std::endl;
  cirrus.reset();

  // TODO: Write out any new stats we need.

  return 0;
}
