#include <iostream>
#include <cstdint>
#include <chrono>

#include <gflags/gflags.h>
#include <nanodbc/nanodbc.h>

DEFINE_string(cstr, "", "The connection string to use.");
DEFINE_string(dbname, "", "The name of the database (used for results).");
DEFINE_uint32(iters, 10, "The number of requests to run in a loop in one timing session.");
DEFINE_uint32(trials, 5, "The number of trials to run.");

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Measure query dispatch overhead from native code.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

  if (FLAGS_cstr.empty()) {
    std::cerr << "ERROR: Specify a connection string." << std::endl;
    return 1;
  }
  if (FLAGS_dbname.empty()) {
    std::cerr << "ERROR: Specify a database name." << std::endl;
    return 1;
  }

  nanodbc::connection c(FLAGS_cstr);

  std::cout << "dbname,iters,run_time_ns" << std::endl;

  for (uint32_t trial = 0; trial < FLAGS_trials; ++trial) {
    const auto start = std::chrono::steady_clock::now();
    for (uint32_t iter = 0; iter < FLAGS_iters; ++iter) {
      nanodbc::execute(c, "SELECT 1");
    }
    const auto end = std::chrono::steady_clock::now();
    std::cout << FLAGS_dbname << "," << FLAGS_iters << "," << (end - start).count() << std::endl;
  }

  return 0;
}
