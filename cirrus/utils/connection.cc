#include "connection.h"

#include <sstream>
#include <string>

DEFINE_string(host, "", "Database server.");
DEFINE_string(dbname, "dev", "Database name.");
DEFINE_string(user, "awsuser", "Database username.");
DEFINE_string(pwdvar, "",
              "Name of the environment variable where the password is saved.");

namespace {

std::string GenerateRedshiftConnectionString() {
  std::stringstream builder;
  builder << "Driver={Amazon Redshift (x64)};";
  builder << " Database=" << FLAGS_dbname << ";";
  builder << " Server=" << FLAGS_host << ";";
  builder << " UID=" << FLAGS_user << ";";
  if (!FLAGS_pwdvar.empty()) {
    builder << " PWD=" << std::getenv(FLAGS_pwdvar.c_str()) << ";";
  }
  return builder.str();
}

std::string GenerateAuroraConnectionString() {
  std::stringstream builder;
  builder << "Driver={PostgreSQL};";
  builder << " Server=" << FLAGS_host << ";";
  builder << " Port=5432;";
  builder << " UID=" << FLAGS_user << ";";
  if (!FLAGS_pwdvar.empty()) {
    builder << " PWD=" << std::getenv(FLAGS_pwdvar.c_str()) << ";";
  }
  return builder.str();
}

}  // namespace

std::string Connection::redshift_connection_str_;
std::string Connection::aurora_connection_str_;

const nanodbc::connection Connection::GetConnection(DBType dbtype) {
  switch (dbtype) {
    case DBType::kRedshift: {
      return nanodbc::connection(GetConnectionString(dbtype));
    }
    case DBType::kAurora: {
      // Temporary workaround. You need to define a data source name called
      // "Aurora" in your local `~/.odbc.ini` file.
      return nanodbc::connection(
          "Aurora", FLAGS_user,
          !FLAGS_pwdvar.empty() ? std::getenv(FLAGS_pwdvar.c_str()) : "");
    }
  }
  __builtin_unreachable();
}

const std::string& Connection::GetConnectionString(DBType dbtype) {
  switch (dbtype) {
    case DBType::kRedshift: {
      if (redshift_connection_str_.empty()) {
        redshift_connection_str_ = GenerateRedshiftConnectionString();
      }
      return redshift_connection_str_;
    }
    case DBType::kAurora: {
      if (aurora_connection_str_.empty()) {
        aurora_connection_str_ = GenerateAuroraConnectionString();
      }
      return aurora_connection_str_;
    }
  }
  __builtin_unreachable();
}
