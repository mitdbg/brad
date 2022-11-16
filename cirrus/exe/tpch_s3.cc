#include <gflags/gflags.h>
#include <nanodbc/nanodbc.h>

#include <iomanip>
#include <iostream>
#include <sstream>

#include "utils/connection.h"
#include "utils/dbtype.h"
#include "utils/sf.h"

DEFINE_uint32(sf, 1, "Scale factor.");
DEFINE_bool(drop, false, "Set to drop the tables instead.");

DEFINE_string(bucket, "geoffxy-research",
              "The S3 bucket where the data is stored.");
DEFINE_string(iam_role, "", "The IAM role to use for copying from S3");

DEFINE_string(db, "redshift", "The DBMS to use.");

namespace {

std::string CreatePart(uint32_t sf) {
  static const std::string prefix = "CREATE TABLE IF NOT EXISTS part_";
  return prefix + PaddedScaleFactor(sf) +
         "("
         "  p_partkey  INTEGER PRIMARY KEY,"
         "  p_name     VARCHAR(55),"
         "  p_mfgr     CHAR(25),"
         "  p_brand    CHAR(10),"
         "  p_type     VARCHAR(25),"
         "  p_size     INTEGER,"
         "  p_container    CHAR(10),"
         "  p_retailprice  DECIMAL,"
         "  p_comment  VARCHAR(23),"
         "  p_extra  CHAR(1)"
         ");";
}

std::string CreateSupplier(uint32_t sf) {
  static const std::string prefix = "CREATE TABLE IF NOT EXISTS supplier_";
  return prefix + PaddedScaleFactor(sf) +
         "("
         "  s_suppkey   INTEGER PRIMARY KEY,"
         "  s_name			CHAR(25),"
         "  s_address   VARCHAR(40),"
         "  s_nationkey BIGINT NOT NULL,"
         "  s_phone     CHAR(15),"
         "  s_acctbal   DECIMAL,"
         "  s_comment   VARCHAR(101),"
         "  s_extra  CHAR(1)"
         ");";
}

std::string CreatePartSupp(uint32_t sf) {
  static const std::string prefix = "CREATE TABLE IF NOT EXISTS partsupp_";
  return prefix + PaddedScaleFactor(sf) +
         "("
         "  ps_partkey     BIGINT NOT NULL,"
         "  ps_suppkey     BIGINT NOT NULL,"
         "  ps_availqty    INTEGER,"
         "  ps_supplycost  DECIMAL,"
         "  ps_comment     VARCHAR(199),"
         "  ps_extra  CHAR(1),"
         "  PRIMARY KEY (ps_partkey, ps_suppkey)"
         ");";
}

std::string CreateCustomer(uint32_t sf) {
  static const std::string prefix = "CREATE TABLE IF NOT EXISTS customer_";
  return prefix + PaddedScaleFactor(sf) +
         "("
         "  c_custkey    INTEGER PRIMARY KEY,"
         "  c_name       VARCHAR(25),"
         "  c_address    VARCHAR(40),"
         "  c_nationkey  BIGINT NOT NULL,"
         "  c_phone      CHAR(15),"
         "  c_acctbal    DECIMAL,"
         "  c_mktsegment CHAR(10),"
         "  c_comment    VARCHAR(117),"
         "  c_extra  CHAR(1)"
         ");";
}

std::string CreateOrders(uint32_t sf) {
  static const std::string prefix = "CREATE TABLE IF NOT EXISTS orders_";
  return prefix + PaddedScaleFactor(sf) +
         "("
         "  o_orderkey     INTEGER PRIMARY KEY,"
         "  o_custkey      BIGINT NOT NULL,"
         "  o_orderstatus  CHAR(1),"
         "  o_totalprice	  DECIMAL,"
         "  o_orderdate    DATE,"
         "  o_orderpriority	CHAR(15),"
         "  o_clerk        CHAR(15),"
         "  o_shippriority INTEGER,"
         "  o_comment      VARCHAR(79),"
         "  o_extra  CHAR(1)"
         ");";
}

std::string CreateLineItem(uint32_t sf) {
  static const std::string prefix = "CREATE TABLE IF NOT EXISTS lineitem_";
  return prefix + PaddedScaleFactor(sf) +
         "("
         "  l_orderkey       BIGINT NOT NULL,"
         "  l_partkey        BIGINT NOT NULL,"
         "  l_suppkey        BIGINT NOT NULL,"
         "  l_linenumber     INTEGER,"
         "  l_quantity       DECIMAL,"
         "  l_extendedprice	DECIMAL,"
         "  l_discount       DECIMAL,"
         "  l_tax            DECIMAL,"
         "  l_returnflag     CHAR(1),"
         "  l_linestatus     CHAR(1),"
         "  l_shipdate       DATE,"
         "  l_commitdate     DATE,"
         "  l_receiptdate    DATE,"
         "  l_shipinstruct	  CHAR(25),"
         "  l_shipmode       CHAR(10),"
         "  l_comment        VARCHAR(44),"
         "  l_extra  CHAR(1),"
         "PRIMARY KEY (l_orderkey, l_linenumber)"
         ");";
}

std::string CreateNation(uint32_t sf) {
  static const std::string prefix = "CREATE TABLE IF NOT EXISTS nation_";
  return prefix + PaddedScaleFactor(sf) +
         "("
         "  n_nationkey    INTEGER PRIMARY KEY,"
         "  n_name         CHAR(25),"
         "  n_regionkey    BIGINT NOT NULL,"
         "  n_comment      VARCHAR(152),"
         "  n_extra  CHAR(1)"
         ");";
}

std::string CreateRegion(uint32_t sf) {
  static const std::string prefix = "CREATE TABLE IF NOT EXISTS region_";
  return prefix + PaddedScaleFactor(sf) +
         "("
         "  r_regionkey  INTEGER PRIMARY KEY,"
         "  r_name       CHAR(25),"
         "  r_comment    VARCHAR(152),"
         "  r_extra  CHAR(1)"
         ");";
}

void CreateTPCHTables(nanodbc::connection& connection, uint32_t sf) {
  nanodbc::transaction txn(connection);
  nanodbc::execute(connection, CreatePart(sf));
  nanodbc::execute(connection, CreateSupplier(sf));
  nanodbc::execute(connection, CreatePartSupp(sf));
  nanodbc::execute(connection, CreateCustomer(sf));
  nanodbc::execute(connection, CreateOrders(sf));
  nanodbc::execute(connection, CreateLineItem(sf));
  nanodbc::execute(connection, CreateNation(sf));
  nanodbc::execute(connection, CreateRegion(sf));
  txn.commit();
}

void DropTPCHTables(nanodbc::connection& connection, uint32_t sf) {
  nanodbc::transaction txn(connection);
  nanodbc::execute(connection,
                   "DROP TABLE IF EXISTS part_" + PaddedScaleFactor(sf));
  nanodbc::execute(connection,
                   "DROP TABLE IF EXISTS supplier_" + PaddedScaleFactor(sf));
  nanodbc::execute(connection,
                   "DROP TABLE IF EXISTS partsupp_" + PaddedScaleFactor(sf));
  nanodbc::execute(connection,
                   "DROP TABLE IF EXISTS customer_" + PaddedScaleFactor(sf));
  nanodbc::execute(connection,
                   "DROP TABLE IF EXISTS orders_" + PaddedScaleFactor(sf));
  nanodbc::execute(connection,
                   "DROP TABLE IF EXISTS lineitem_" + PaddedScaleFactor(sf));
  nanodbc::execute(connection,
                   "DROP TABLE IF EXISTS nation_" + PaddedScaleFactor(sf));
  nanodbc::execute(connection,
                   "DROP TABLE IF EXISTS region_" + PaddedScaleFactor(sf));
  txn.commit();
}

std::string GenerateCopyCommand(const std::string& table_name, uint32_t sf) {
  std::stringstream builder;
  builder << "COPY " << table_name << "_" << PaddedScaleFactor(sf);
  builder << " FROM 's3://" << FLAGS_bucket << "/tpch/sf"
          << PaddedScaleFactor(sf) << "/" << table_name << ".tbl'";
  builder << " IAM_ROLE '" << FLAGS_iam_role << "'";
  builder << " REGION 'us-east-1'";
  return builder.str();
}

std::string GenerateAuroraCopyCommand(const std::string& table_name,
                                      uint32_t sf) {
  /*
  SELECT aws_s3.table_import_from_s3(
    'region_001',
    '',
    'DELIMITER ''|''',
    aws_commons.create_s3_uri('geoffxy-research', 'tpch/sf001/region.tbl',
  'us-east-1')
  );
  */
  std::stringstream builder;
  builder << "SELECT aws_s3.table_import_from_s3(";
  builder << "'" << table_name << "_" << PaddedScaleFactor(sf) << "',";
  builder << "'',";
  builder << "'DELIMITER ''|''',";
  builder << "aws_commons.create_s3_uri('geoffxy-research', 'tpch/sf"
          << PaddedScaleFactor(sf) << "/" << table_name
          << ".tbl', 'us-east-1')";
  builder << ");";
  return builder.str();
}

void LoadData(nanodbc::connection& connection, uint32_t sf) {
  std::cerr << "> Loading part..." << std::endl;
  nanodbc::execute(connection, GenerateCopyCommand("part", sf));
  std::cerr << "> Loading supplier..." << std::endl;
  nanodbc::execute(connection, GenerateCopyCommand("supplier", sf));
  std::cerr << "> Loading partsupp..." << std::endl;
  nanodbc::execute(connection, GenerateCopyCommand("partsupp", sf));
  std::cerr << "> Loading customer..." << std::endl;
  nanodbc::execute(connection, GenerateCopyCommand("customer", sf));
  std::cerr << "> Loading orders..." << std::endl;
  nanodbc::execute(connection, GenerateCopyCommand("orders", sf));
  std::cerr << "> Loading lineitem..." << std::endl;
  nanodbc::execute(connection, GenerateCopyCommand("lineitem", sf));
  std::cerr << "> Loading nation..." << std::endl;
  nanodbc::execute(connection, GenerateCopyCommand("nation", sf));
  std::cerr << "> Loading region..." << std::endl;
  nanodbc::execute(connection, GenerateCopyCommand("region", sf));
}

void LoadDataAurora(nanodbc::connection& connection, uint32_t sf) {
  std::cerr << "> Loading part..." << std::endl;
  nanodbc::execute(connection, GenerateAuroraCopyCommand("part", sf));
  std::cerr << "> Loading supplier..." << std::endl;
  nanodbc::execute(connection, GenerateAuroraCopyCommand("supplier", sf));
  std::cerr << "> Loading partsupp..." << std::endl;
  nanodbc::execute(connection, GenerateAuroraCopyCommand("partsupp", sf));
  std::cerr << "> Loading customer..." << std::endl;
  nanodbc::execute(connection, GenerateAuroraCopyCommand("customer", sf));
  std::cerr << "> Loading orders..." << std::endl;
  nanodbc::execute(connection, GenerateAuroraCopyCommand("orders", sf));
  std::cerr << "> Loading lineitem..." << std::endl;
  nanodbc::execute(connection, GenerateAuroraCopyCommand("lineitem", sf));
  std::cerr << "> Loading nation..." << std::endl;
  nanodbc::execute(connection, GenerateAuroraCopyCommand("nation", sf));
  std::cerr << "> Loading region..." << std::endl;
  nanodbc::execute(connection, GenerateAuroraCopyCommand("region", sf));
}

}  // namespace

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage(
      "Used to load TPC-H data (on S3) into AWS databases.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

  if (!FLAGS_drop && FLAGS_iam_role.empty()) {
    std::cerr << "ERROR: Must specify --iam_role to copy data from S3."
              << std::endl;
    return 1;
  }

  const auto maybe_db = FromString(FLAGS_db);
  if (!maybe_db.has_value()) {
    std::cerr << "ERROR: Unrecognized database " << FLAGS_db << std::endl;
    return 1;
  }

  DBType db = *maybe_db;
  nanodbc::connection c(utils::GetConnection());

  if (!FLAGS_drop) {
    std::cerr << "> Creating the tables..." << std::endl;
    CreateTPCHTables(c, FLAGS_sf);

    std::cerr << "> Loading data from s3://" << FLAGS_bucket << std::endl;
    if (db == DBType::kRedshift) {
      LoadData(c, FLAGS_sf);
    } else {
      LoadDataAurora(c, FLAGS_sf);
    }

  } else {
    std::cerr << "> Dropping the tables..." << std::endl;
    DropTPCHTables(c, FLAGS_sf);
  }

  return 0;
}
