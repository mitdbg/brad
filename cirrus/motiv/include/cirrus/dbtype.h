#pragma once

#include <optional>
#include <string>

namespace cirrus {

enum class DBType {
  kRedshift,
  kRDSPostgreSQL,
};

std::optional<DBType> DBTypeFromString(const std::string& candidate);
std::string DBTypeToString(DBType dbtype);

}  // namespace cirrus
