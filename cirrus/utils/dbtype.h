#pragma once

#include <optional>
#include <string>

enum class DBType {
  kRedshift,
  kRDSPostgreSQL,
};

namespace dbtype {

std::optional<DBType> FromString(const std::string& candidate);
std::string ToString(DBType dbtype);

}  // namespace dbtype
