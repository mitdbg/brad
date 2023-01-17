#include <gflags/gflags.h>

#include <chrono>
#include <iostream>

#include "cirrus/config.h"
#include "cirrus/odbc.h"

using namespace cirrus;

// Can also set these flags instead of `--config_file`.
DEFINE_string(dsn, "", "ODBC DSN for local connections.");
DEFINE_string(user, "", "ODBC username for local connections.");
DEFINE_string(pwdvar, "", "ODBC password variable for local connections.");

DEFINE_uint32(sf, 0, "Specifies the dataset scale factor.");
DEFINE_uint32(trials, 3, "Number of trials.");
DEFINE_string(iam_role, "", "Used for Redshift importing.");

namespace {

static const std::string kCreateImportTable =
    "CREATE TABLE IF NOT EXISTS inventory_wide_hot(LIKE inventory_wide))";

static const std::string kTruncateImportTable =
    "TRUNCATE TABLE inventory_wide_hot";

std::string GenerateImportQuery(const std::string& iam_role, uint32_t sf) {
  std::stringstream query;
  query << "COPY inventory_wide_hot"
        << " FROM 's3://geoffxy-research/etl/invslide/invslide-" << sf
        << ".tbl'"
        << " IAM_ROLE '" << iam_role << "' REGION 'us-east-1'";
  return query.str();
}

}  // namespace

int main(int argc, char* argv[]) {
  // Hypothesis is that import cost is linear with respect to input size, with
  // an initial fixed import time.
  gflags::SetUsageMessage(
      "Used to benchmark Redshift S3 ingestion performance.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

  if (FLAGS_sf == 0) {
    std::cerr << "ERROR: Please set the scale factor --sf." << std::endl;
    return 1;
  }

  auto config = CirrusConfig::Local(FLAGS_dsn, FLAGS_user, FLAGS_pwdvar);

  nanodbc::connection c = GetOdbcConnection(*config, config->read_store_type());

  const std::string import_query = GenerateImportQuery(FLAGS_iam_role, FLAGS_sf);
  std::cerr << "> Starting experiment..." << std::endl;
  nanodbc::execute(c, kCreateImportTable);

  std::cout << "sf,import_time_ms" << std::endl;
  for (uint32_t i = 0; i < FLAGS_trials; ++i) {
    nanodbc::execute(c, kTruncateImportTable);
    const auto start = std::chrono::steady_clock::now();
    nanodbc::execute(c, import_query);
    const auto end = std::chrono::steady_clock::now();
    std::cout << FLAGS_sf << "," << (end - start).count() / 1e6 << std::endl;
  }

  return 0;
}
