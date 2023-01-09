#include "dataset_admin.h"

#include <fstream>
#include <memory>
#include <random>
#include <vector>

#include "column_gen.h"
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

void DatasetAdmin::GenerateTo(const std::filesystem::path& output_path,
                              uint32_t seed) const {
  std::mt19937 prng(seed);

  for (const auto& table : state_->config_["tables"]) {
    std::vector<std::unique_ptr<ColumnGenerator>> gens;

    const std::string table_name = table["name"].as<std::string>();
    const uint64_t multiplier = table["multiplier"].as<uint64_t>();

    // Parse columns.
    for (const auto& col : table["columns"]) {
      std::unique_ptr<ColumnGenerator> generator;
      const std::string dist_type = col["dist"]["type"].as<std::string>();
      if (dist_type == "increment") {
        generator =
            std::unique_ptr<ColumnGenerator>(new IncrementColumnGenerator(
                col["dist"]["start_from"].as<uint64_t>()));
      } else if (dist_type == "uniform") {
        generator = std::unique_ptr<ColumnGenerator>(
            new UniformColumnGenerator(col["dist"]["min"].as<uint64_t>(),
                                       col["dist"]["max"].as<uint64_t>()));
      } else if (dist_type == "increasing") {
        generator =
            std::unique_ptr<ColumnGenerator>(new IncreasingColumnGenerator(
                col["dist"]["start_from"].as<uint64_t>(),
                col["dist"]["max_gap"].as<uint64_t>()));
      }
      gens.push_back(std::move(generator));

      // TODO: Support foreign key.
    }

    const uint64_t num_rows = multiplier * scale_factor_;
    std::ofstream out(output_path / (table_name + ".tbl"));
    for (uint64_t i = 0; i < num_rows; ++i) {
      for (uint64_t j = 0; j < gens.size(); ++j) {
        gens[i]->WriteNext(out, prng);
        if (j < gens.size() - 1) {
          out << "|";
        }
      }
      out << std::endl;
    }
  }
}

void DatasetAdmin::LoadFromS3(nanodbc::connection& db, DBType dbtype,
                              const std::string& bucket) const {}

void DatasetAdmin::ResetToGenerated(nanodbc::connection& db,
                                    DBType dbtype) const {}

}  // namespace cirrus
