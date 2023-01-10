#include <gflags/gflags.h>

#include <iostream>

#include "cirrus/config.h"
#include "cirrus/dbtype.h"
#include "cirrus/odbc.h"
#include "dataset_admin.h"

DEFINE_string(action, "generate", "What to do. One of {generate, load}.");

DEFINE_string(config, "", "Path to the dataset configuration file.");
DEFINE_uint32(sf, 1, "The scale factor to use.");

// `generate`-specific flags
DEFINE_string(out_path, "", "Where to output the generated files.");

// `load`-specific flags
DEFINE_string(bucket, "", "S3 bucket (for loading).");
DEFINE_string(iam_role, "", "AWS IAM role (for loading to RDS).");
DEFINE_string(db, "", "The database type. Needs to be set when loading data.");
DEFINE_string(
    config_file, "",
    "Path to a Cirrus config file. Needs to be set when loading data.");
// Can also set these flags instead of `--config_file`.
DEFINE_string(dsn, "", "ODBC DSN for local connections.");
DEFINE_string(user, "", "ODBC username for local connections.");
DEFINE_string(pwdvar, "", "ODBC password variable for local connections.");

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Handles generating and loading data.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

  cirrus::DatasetAdmin dataset(FLAGS_config, FLAGS_sf);

  if (FLAGS_action == "generate") {
    dataset.GenerateTo(FLAGS_out_path);

  } else if (FLAGS_action == "load") {
    const auto maybe_db = cirrus::DBTypeFromString(FLAGS_db);
    if (!maybe_db.has_value()) {
      std::cerr << "ERROR: Unrecognized DB " << FLAGS_db << std::endl;
      return 0;
    }
    cirrus::DBType db = *maybe_db;

    auto config =
        !FLAGS_config_file.empty()
            ? cirrus::CirrusConfig::LoadFrom(FLAGS_config_file)
            : cirrus::CirrusConfig::Local(FLAGS_dsn, FLAGS_user, FLAGS_pwdvar);
    nanodbc::connection c = cirrus::GetOdbcConnection(*config, db);

    dataset.CreateTables(c, db);
    dataset.LoadFromS3(c, db, FLAGS_bucket, FLAGS_iam_role);

  } else {
    std::cerr << "ERROR: Unrecognized action " << FLAGS_action << std::endl;
    return 1;
  }

  return 0;
}
