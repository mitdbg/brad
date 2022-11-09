#include "connection.h"

std::string Connection::connection_str_;

const std::string& Connection::GetConnectionString() {
  return connection_str_;
}

void Connection::SetConnectionString(std::string value) {
  connection_str_ = std::move(value);
}
