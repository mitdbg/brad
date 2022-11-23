#include "connection.h"

#include <cstdlib>

DEFINE_string(default_odbc_dsn, "",
              "The default data source name to use, defined in ~/.odbc.ini.");
DEFINE_string(default_user, "", "Default database username.");
// We assume we use the same password across all the databases.
DEFINE_string(pwdvar, "",
              "The environment variable that stores the user's password.");

DEFINE_string(redshift_odbc_dsn, "Amazon Redshift DSN 64",
              "The data source name to use to connect to Redshift, defined in "
              "~/.odbc.ini.");
DEFINE_string(redshift_user, "awsuser", "The Redshift username.");

DEFINE_string(
    pg_odbc_dsn, "RDS PostgreSQL",
    "The data source name to use to connect to PostgreSQL, defined in "
    "~/.odbc.ini.");
DEFINE_string(pg_user, "postgres", "The PostgreSQL username.");

DEFINE_string(pg_replica_odbc_dsn, "RDS PostgreSQL Replica",
              "The data source name to use to connect to a PostgreSQL read "
              "replica, defined in ~/.odbc.ini.");

namespace utils {

nanodbc::connection GetConnection() {
  return nanodbc::connection(
      FLAGS_default_odbc_dsn, FLAGS_default_user,
      !FLAGS_pwdvar.empty() ? std::getenv(FLAGS_pwdvar.c_str()) : "");
}

nanodbc::connection GetConnection(DBType dbtype) {
  switch (dbtype) {
    case DBType::kRDSPostgreSQL: {
      return nanodbc::connection(
          FLAGS_pg_odbc_dsn, FLAGS_pg_user,
          !FLAGS_pwdvar.empty() ? std::getenv(FLAGS_pwdvar.c_str()) : "");
    }
    case DBType::kRDSPostgreSQLReplica: {
      return nanodbc::connection(
          FLAGS_pg_replica_odbc_dsn, FLAGS_pg_user,
          !FLAGS_pwdvar.empty() ? std::getenv(FLAGS_pwdvar.c_str()) : "");
    }
    case DBType::kRedshift: {
      return nanodbc::connection(
          FLAGS_redshift_odbc_dsn, FLAGS_redshift_user,
          !FLAGS_pwdvar.empty() ? std::getenv(FLAGS_pwdvar.c_str()) : "");
    }
  }
  __builtin_unreachable();
}

}  // namespace utils
