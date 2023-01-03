#include "cirrus/dbtype.h"

#include <cassert>
#include <unordered_map>

namespace cirrus {

std::optional<DBType> DBTypeFromString(const std::string& candidate) {
  static const std::unordered_map<std::string, DBType> kMap = {
      {"redshift", DBType::kRedshift}, {"rdspg", DBType::kRDSPostgreSQL}};
  const auto it = kMap.find(candidate);
  if (it == kMap.end()) {
    return std::optional<DBType>();
  }
  return it->second;
}

std::string DBTypeToString(DBType dbtype) {
  static const std::unordered_map<DBType, std::string> kMap = {
      {DBType::kRedshift, "redshift"}, {DBType::kRDSPostgreSQL, "rdspg"}};
  const auto it = kMap.find(dbtype);
  assert(it != kMap.end());
  return it->second;
}

}  // namespace cirrus
