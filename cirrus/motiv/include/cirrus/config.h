#pragma once

#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>

#include "cirrus/dbtype.h"

namespace cirrus {

class CirrusConfig {
 public:
  // Throws an exception if loading the config fails.
  static std::shared_ptr<CirrusConfig> LoadFrom(
      const std::filesystem::path& path);

  // Used for local testing.
  static std::shared_ptr<CirrusConfig> Local(const std::string& dsn,
                                             const std::string& username,
                                             const std::string& pwdvar);

  virtual DBType read_store_type() const = 0;
  virtual DBType write_store_type() const = 0;

  virtual std::string odbc_dsn(DBType dbtype) const = 0;
  virtual std::string odbc_user(DBType dbtype) const = 0;
  virtual std::string odbc_pwd(DBType dbtype) const = 0;

  virtual size_t bg_workers() const = 0;

  virtual std::string iam_role() const = 0;
};

}  // namespace cirrus
