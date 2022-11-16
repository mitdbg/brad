#include <gflags/gflags.h>
#include <nanodbc/nanodbc.h>

#include <chrono>
#include <iostream>
#include <optional>
#include <sstream>
#include <unordered_map>

#include "datasets/store.h"
#include "utils/connection.h"
#include "utils/dbtype.h"
#include "utils/sf.h"
#include "workloads/state.h"
#include "workloads/store.h"

DEFINE_string(
    action, "",
    "What to do {generate, load_from_s3, generate_load_directly, drop}.");

DEFINE_string(gen_out, "",
              "Path to where the generated data should be written.");
DEFINE_string(db, "", "The database type. Needs to be set when loading data.");

DEFINE_uint32(sf, 0, "Specifies the dataset scale factor.");

DEFINE_string(iam_role, "", "Needs to be set for Redshift loads.");
DEFINE_string(s3_bucket, "", "Needs to be set when loading from S3.");

namespace {

std::string GenerateRedshiftS3LoadCommand(const std::string& prefix,
                                          const std::string& table_name,
                                          uint32_t sf) {
  std::stringstream builder;
  builder << "COPY " << table_name;
  builder << " FROM 's3://" << FLAGS_s3_bucket << "/" << prefix << "sf"
          << PaddedScaleFactor(sf) << "/" << table_name << ".tbl'";
  builder << " IAM_ROLE '" << FLAGS_iam_role << "'";
  builder << " REGION 'us-east-1'";
  return builder.str();
}

std::string GenerateRDSS3LoadCommand(const std::string& prefix,
                                     const std::string& table_name,
                                     uint32_t sf) {
  /*
  SELECT aws_s3.table_import_from_s3(
    'region_001',
    '',
    'DELIMITER ''|''',
    aws_commons.create_s3_uri('bucket', 'tpch/sf001/region.tbl',
  'us-east-1')
  );
  */
  std::stringstream builder;
  builder << "SELECT aws_s3.table_import_from_s3(";
  builder << "'" << table_name << "_" << PaddedScaleFactor(sf) << "',";
  builder << "'',";
  builder << "'DELIMITER ''|''',";
  builder << "aws_commons.create_s3_uri('" << FLAGS_s3_bucket << "', '"
          << prefix << "sf" << PaddedScaleFactor(sf) << "/" << table_name
          << ".tbl', 'us-east-1')";
  builder << ");";
  return builder.str();
}

}  // namespace

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage(
      "Handles generating and loading data for the 'store' dataset.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

  if (FLAGS_sf == 0) {
    std::cerr << "ERROR: Please set the scale factor --sf." << std::endl;
    return 1;
  }

  StoreDataset store(FLAGS_sf);

  if (FLAGS_action == "generate") {
    // Generate the dataset and save it in text files.
    if (FLAGS_gen_out.empty()) {
      return 1;
    }
    std::filesystem::path out(FLAGS_gen_out);
    store.GenerateDataFiles(out);
    return 0;
  }

  nanodbc::connection c(utils::GetConnection());
  if (FLAGS_action == "connect") {
    std::cout << "Connected using ODBC DSN: " << FLAGS_odbc_dsn << std::endl;
    return 0;
  }

  if (FLAGS_action == "load_from_s3") {
    if (FLAGS_db.empty()) {
      std::cerr << "ERROR: Need to specify the DB." << std::endl;
      return 1;
    }
    const auto maybe_db = dbtype::FromString(FLAGS_db);
    if (!maybe_db.has_value()) {
      std::cerr << "ERROR: Unrecognized DB " << FLAGS_db << std::endl;
      return 0;
    }
    DBType db = *maybe_db;

    store.CreateTables(c);
    nanodbc::transaction txn(c);
    if (db == DBType::kRedshift) {
      nanodbc::execute(
          c, GenerateRedshiftS3LoadCommand("store/", "inventory", FLAGS_sf));
      nanodbc::execute(
          c, GenerateRedshiftS3LoadCommand("store/", "sales", FLAGS_sf));
    } else {
      nanodbc::execute(
          c, GenerateRDSS3LoadCommand("store/", "inventory", FLAGS_sf));
      nanodbc::execute(c,
                       GenerateRDSS3LoadCommand("store/", "sales", FLAGS_sf));
    }
    txn.commit();
    return 0;
  }

  if (FLAGS_action == "generate_load_directly") {
    store.CreateTables(c);
    store.GenerateAndLoad(c);
    return 0;
  }

  if (FLAGS_action == "drop") {
    store.DropAll(c);
    return 0;
  }

  std::cerr << "Unrecognized action: " << FLAGS_action << std::endl;
  return 1;
}
