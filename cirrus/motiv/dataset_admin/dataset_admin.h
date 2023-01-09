#pragma once

#include <nanodbc/nanodbc.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

#include "cirrus/dbtype.h"

namespace cirrus {

class DatasetAdmin {
 public:
  DatasetAdmin(const std::filesystem::path& config_file, uint32_t scale_factor);
  ~DatasetAdmin();

  void GenerateTo(const std::filesystem::path& output_path) const;
  void LoadFromS3(nanodbc::connection& db, DBType dbtype,
                  const std::string& bucket) const;

  void ResetToGenerated(nanodbc::connection& db, DBType dbtype) const;

 private:
  void GenerateTable(const std::filesystem::path& output_path,
                     const std::string& table_name) const;

  class State;
  std::unique_ptr<State> state_;
  uint32_t scale_factor_;
};

}  // namespace cirrus
