#include <gflags/gflags.h>
#include <nanodbc/nanodbc.h>

#include <iomanip>
#include <sstream>

#include "connection.h"

namespace {

std::string PaddedScaleFactor(uint32_t sf) {
  std::stringstream builder;
  builder << std::setfill('0') << std::setw(3) << sf;
  return builder.str();
}

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
         "  p_comment  VARCHAR(23)"
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
         "  s_comment   VARCHAR(101)"
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
         "  c_comment    VARCHAR(117)"
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
         "  o_comment      VARCHAR(79)"
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
         "  n_comment      VARCHAR(152)"
         ");";
}

std::string CreateRegion(uint32_t sf) {
  static const std::string prefix = "CREATE TABLE IF NOT EXISTS region_";
  return prefix + PaddedScaleFactor(sf) +
         "("
         "  r_regionkey  INTEGER PRIMARY KEY,"
         "  r_name       CHAR(25),"
         "  r_comment    VARCHAR(152)"
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

}  // namespace

DEFINE_uint32(sf, 1, "Scale factor.");
DEFINE_bool(drop, false, "Set to drop the tables instead.");

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Used to load TPC-H data (on S3) into Redshift.");
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

  Connection::InitConnectionString();
  auto const connstr = NANODBC_TEXT(Connection::GetConnectionString());
  nanodbc::connection c(connstr);

  if (!FLAGS_drop) {
    CreateTPCHTables(c, FLAGS_sf);
  } else {
    DropTPCHTables(c, FLAGS_sf);
  }

  return 0;
}
