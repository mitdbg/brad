#pragma once

#include <string>

class Connection {
 public:
  static const std::string& GetConnectionString();
  static void SetConnectionString(std::string value);

 private:
  static std::string connection_str_;
};
