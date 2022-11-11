#pragma once

#include <optional>
#include <string>

enum class DBType {
  kRedshift,
  kAurora,
};

std::optional<DBType> FromString(const std::string& candidate);
std::string ToString(DBType dbtype);
