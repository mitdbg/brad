#include "connection.h"

#include <string>
#include <sstream>

DEFINE_string(host, "", "Database server.");
DEFINE_string(dbname, "dev", "Database name.");
DEFINE_string(user, "awsuser", "Database username.");
DEFINE_string(pwdvar, "",
              "Name of the environment variable where the password is saved.");

namespace {

std::string GenerateOptionsString() {
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

}

std::string Connection::connection_str_;

void Connection::InitConnectionString() {
  connection_str_ = GenerateOptionsString();
}

const std::string& Connection::GetConnectionString() {
  return connection_str_;
}
