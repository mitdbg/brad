#include "dataset_admin.h"

#include "yaml-cpp/yaml.h"

namespace cirrus {

struct DatasetAdmin::State {
  State(YAML::Node config) : config_(std::move(config)) {}
  YAML::Node config_;
};

DatasetAdmin::~DatasetAdmin() = default;

DatasetAdmin::DatasetAdmin(const std::filesystem::path& config_file,
                           uint32_t scale_factor)
    : state_(std::make_unique<State>(YAML::LoadFile(config_file))),
      scale_factor_(scale_factor) {}

void DatasetAdmin::GenerateTo(const std::filesystem::path& output_path) const {
}

void DatasetAdmin::LoadFromS3(nanodbc::connection& db, DBType dbtype,
                              const std::string& bucket) const {}

void DatasetAdmin::ResetToGenerated(nanodbc::connection& db,
                                    DBType dbtype) const {}

void DatasetAdmin::GenerateTable(const std::filesystem::path& output_path,
                                 const std::string& table_name) const {}

}  // namespace cirrus
