#pragma once

#include <gflags/gflags.h>

#include <string>

DECLARE_string(host);
DECLARE_string(dbname);
DECLARE_string(user);
DECLARE_string(pwdvar);

class Connection {
 public:
  static void InitConnectionString();
  static const std::string& GetConnectionString();

 private:
  static std::string connection_str_;
};
