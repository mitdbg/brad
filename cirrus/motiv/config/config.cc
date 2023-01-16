#include "cirrus/config.h"

#include <cstdlib>
#include <string>

#include "yaml-cpp/yaml.h"

namespace {

using namespace cirrus;

static const std::string kReadStoreKey = "read_store";
static const std::string kWriteStoreKey = "write_store";

static const std::string kOdbcDsnKey = "odbc_dsn";
static const std::string kOdbcUserKey = "odbc_user";
static const std::string kOdbcPwdvarKey = "odbc_pwdvar";

static const std::string kBgWorkers = "bg_workers";

std::string ExtractEnvVar(const std::string& env_var) {
  return std::getenv(env_var.c_str());
}

bool Validate(const YAML::Node& raw) {
  if (!raw.IsMap()) {
    throw std::runtime_error("Cirrus' configuration needs to be a YAML map.");
  }
  // TODO: More robust validation.
  return true;
}

}  // namespace

namespace cirrus {

class CirrusYAMLConfig : public CirrusConfig {
 public:
  CirrusYAMLConfig(YAML::Node raw);
  DBType read_store_type() const override;
  DBType write_store_type() const override;

  std::string odbc_dsn(DBType dbtype) const override;
  std::string odbc_user(DBType dbtype) const override;
  std::string odbc_pwd(DBType dbtype) const override;

  size_t bg_workers() const override;

  std::string iam_role() const override;

 private:
  mutable std::mutex mutex_;
  YAML::Node raw_config_;
};

class CirrusLocalConfig : public CirrusConfig {
 public:
  CirrusLocalConfig(const std::string& dsn, const std::string& username,
                    const std::string& pwdvar);

  DBType read_store_type() const override;
  DBType write_store_type() const override;

  std::string odbc_dsn(DBType dbtype) const override;
  std::string odbc_user(DBType dbtype) const override;
  std::string odbc_pwd(DBType dbtype) const override;

  size_t bg_workers() const override { return 1; }

  std::string iam_role() const override;

 private:
  std::string dsn_, username_, pwdvar_;
};

std::shared_ptr<CirrusConfig> CirrusConfig::LoadFrom(
    const std::filesystem::path& path) {
  try {
    YAML::Node node = YAML::LoadFile(path);
    if (!Validate(node)) {
      throw std::runtime_error("Invalid Cirrus configuration file.");
    }
    return std::make_shared<CirrusYAMLConfig>(std::move(node));
  } catch (const YAML::BadFile&) {
    throw std::runtime_error("Could not parse the Cirrus configuration file.");
  }
}

std::shared_ptr<CirrusConfig> CirrusConfig::Local(const std::string& dsn,
                                                  const std::string& username,
                                                  const std::string& pwdvar) {
  return std::make_shared<CirrusLocalConfig>(dsn, username, pwdvar);
}

// TODO: Later on we should completely validate the configuration instead of
// throwing exceptions here.

CirrusYAMLConfig::CirrusYAMLConfig(YAML::Node raw)
    : raw_config_(std::move(raw)) {}

DBType CirrusYAMLConfig::read_store_type() const {
  std::unique_lock<std::mutex> lock(mutex_);
  const std::string dbtype_str = raw_config_[kReadStoreKey].as<std::string>();
  const auto maybe_dbtype = DBTypeFromString(dbtype_str);
  if (!maybe_dbtype.has_value()) {
    throw std::runtime_error("Invalid read store type: " + dbtype_str);
  }
  return *maybe_dbtype;
}

DBType CirrusYAMLConfig::write_store_type() const {
  std::unique_lock<std::mutex> lock(mutex_);
  const std::string dbtype_str = raw_config_[kWriteStoreKey].as<std::string>();
  const auto maybe_dbtype = DBTypeFromString(dbtype_str);
  if (!maybe_dbtype.has_value()) {
    throw std::runtime_error("Invalid write store type: " + dbtype_str);
  }
  return *maybe_dbtype;
}

std::string CirrusYAMLConfig::odbc_dsn(DBType dbtype) const {
  std::unique_lock<std::mutex> lock(mutex_);
  return raw_config_[DBTypeToString(dbtype)][kOdbcDsnKey].as<std::string>();
}

std::string CirrusYAMLConfig::odbc_user(DBType dbtype) const {
  std::unique_lock<std::mutex> lock(mutex_);
  return raw_config_[DBTypeToString(dbtype)][kOdbcUserKey].as<std::string>();
}

std::string CirrusYAMLConfig::odbc_pwd(DBType dbtype) const {
  std::unique_lock<std::mutex> lock(mutex_);
  return ExtractEnvVar(
      raw_config_[DBTypeToString(dbtype)][kOdbcPwdvarKey].as<std::string>());
}

size_t CirrusYAMLConfig::bg_workers() const {
  std::unique_lock<std::mutex> lock(mutex_);
  return raw_config_[kBgWorkers].as<size_t>();
}

std::string CirrusYAMLConfig::iam_role() const {
  std::unique_lock<std::mutex> lock(mutex_);
  return raw_config_["iam_role"].as<std::string>();
}

CirrusLocalConfig::CirrusLocalConfig(const std::string& dsn,
                                     const std::string& username,
                                     const std::string& pwdvar)
    : dsn_(dsn), username_(username), pwdvar_(pwdvar) {}

DBType CirrusLocalConfig::read_store_type() const {
  return DBType::kRDSPostgreSQL;
}
DBType CirrusLocalConfig::write_store_type() const {
  return DBType::kRDSPostgreSQL;
}

std::string CirrusLocalConfig::odbc_dsn(DBType dbtype) const { return dsn_; }

std::string CirrusLocalConfig::odbc_user(DBType dbtype) const {
  return username_;
}

std::string CirrusLocalConfig::odbc_pwd(DBType dbtype) const {
  return ExtractEnvVar(pwdvar_);
}

std::string CirrusLocalConfig::iam_role() const { return ""; }

}  // namespace cirrus
