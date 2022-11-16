#include "connection.h"

#include <cstdlib>

DEFINE_string(odbc_dsn, "",
              "The data source name to use, defined in ~/.odbc.ini.");
DEFINE_string(user, "awsuser", "Database username.");
DEFINE_string(pwdvar, "",
              "The environment variable that stores the user's password.");

namespace utils {

nanodbc::connection GetConnection() {
  return nanodbc::connection(
      FLAGS_odbc_dsn, FLAGS_user,
      !FLAGS_pwdvar.empty() ? std::getenv(FLAGS_pwdvar.c_str()) : "");
}

}  // namespace utils
