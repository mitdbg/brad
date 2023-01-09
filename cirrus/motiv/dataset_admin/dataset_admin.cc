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
  std::unordered_map<std::string, uint64_t> pkey_max;

  for (const auto& table : state_->config_["tables"]) {
    std::vector<std::unique_ptr<ColumnGenerator>> gens;

    const std::string table_name = table["name"].as<std::string>();
    const uint64_t multiplier = table["multiplier"].as<uint64_t>();
    const uint64_t num_rows = multiplier * scale_factor_;

    // Parse columns.
    for (const auto& col : table["columns"]) {
      std::unique_ptr<ColumnGenerator> generator;
      const std::string dist_type = col["dist"]["type"].as<std::string>();
      if (dist_type == "primary_key") {
        generator = std::unique_ptr<ColumnGenerator>(
            new IncrementColumnGenerator(/*start_from=*/1));
        const std::string full_name =
            table_name + "." + col["name"].as<std::string>();
        pkey_max[full_name] = num_rows;

      } else if (dist_type == "uniform") {
        generator = std::unique_ptr<ColumnGenerator>(
            new UniformColumnGenerator(col["dist"]["min"].as<uint64_t>(),
                                       col["dist"]["max"].as<uint64_t>()));

      } else if (dist_type == "increasing") {
        generator =
            std::unique_ptr<ColumnGenerator>(new IncreasingColumnGenerator(
                col["dist"]["start_from"].as<uint64_t>(),
                col["dist"]["max_gap"].as<uint64_t>()));

      } else if (dist_type == "foreign_key") {
        const std::string references =
            col["dist"]["references"].as<std::string>();
        const auto it = pkey_max.find(references);
        if (it == pkey_max.end()) {
          throw std::runtime_error("Unknown column " + references);
        }
        generator = std::unique_ptr<ColumnGenerator>(
            new UniformColumnGenerator(1, it->second));
      }
      gens.push_back(std::move(generator));
    }

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
