#include "dbtype.h"

#include <cassert>
#include <unordered_map>

std::optional<DBType> FromString(const std::string& candidate) {
  static const std::unordered_map<std::string, DBType> kMap = {
      {"redshift", DBType::kRedshift}, {"aurora", DBType::kAurora}};
  const auto it = kMap.find(candidate);
  if (it == kMap.end()) {
    return std::optional<DBType>();
  }
  return it->second;
}

std::string ToString(DBType dbtype) {
  static const std::unordered_map<DBType, std::string> kMap = {
      {DBType::kRedshift, "redshift"}, {DBType::kAurora, "aurora"}};
  const auto it = kMap.find(dbtype);
  assert(it != kMap.end());
  return it->second;
}
