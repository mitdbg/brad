#pragma once

#include <gflags/gflags.h>

#include <string>

#include "dbtype.h"

DECLARE_string(host);
DECLARE_string(dbname);
DECLARE_string(user);
DECLARE_string(pwdvar);

class Connection {
 public:
  static const std::string& GetConnectionString(DBType dbtype);

 private:
  static std::string redshift_connection_str_;
  static std::string aurora_connection_str_;
};
