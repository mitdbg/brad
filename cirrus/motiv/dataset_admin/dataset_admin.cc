#include "dataset_admin.h"

#include <fstream>
#include <memory>
#include <optional>
#include <random>
#include <sstream>
#include <vector>

#include "column_gen.h"
#include "utils/sf.h"
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

      } else if (dist_type == "sequence") {
        generator = std::unique_ptr<ColumnGenerator>(
            new IncrementColumnGenerator(/*start_from=*/1));

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

      } else {
        throw std::runtime_error("Unknown column type: " + dist_type);
      }
      gens.push_back(std::move(generator));
    }

    std::ofstream out(output_path / (table_name + ".tbl"));
    for (uint64_t i = 0; i < num_rows; ++i) {
      for (uint64_t j = 0; j < gens.size(); ++j) {
        gens[j]->WriteNext(out, prng);
        if (j < gens.size() - 1) {
          out << "|";
        }
      }
      out << std::endl;
    }
  }
}

void DatasetAdmin::CreateTables(nanodbc::connection& db, DBType dbtype) {
  nanodbc::transaction txn(db);

  // (table, column name)
  std::vector<std::pair<std::string, std::string>> sequence_cols;

  for (const auto& table : state_->config_["tables"]) {
    const std::string table_name = table["name"].as<std::string>();
    std::vector<std::pair<std::string, std::string>> columns;
    std::string pkey_column;  // TODO: Assumes a single primary key column.

    // Parse all columns.
    for (const auto& col : table["columns"]) {
      const std::string col_name = col["name"].as<std::string>();
      columns.push_back({col_name, col["type"].as<std::string>()});
      if (col["dist"]["type"].as<std::string>() == "primary_key") {
        pkey_column = col_name;
      }

      if (col["dist"]["type"].as<std::string>() == "sequence") {
        sequence_cols.push_back({table_name, col["name"].as<std::string>()});
      }
    }

    // Generate the create table SQL.
    std::stringstream query;
    query << "CREATE TABLE IF NOT EXISTS " << table_name << " (";
    for (const auto& col : columns) {
      query << col.first << " " << col.second << ", ";
    }
    query << "PRIMARY KEY (" << pkey_column << "));";

    nanodbc::execute(db, query.str());
  }

  // Create indexes on the sequence columns.
  if (dbtype != DBType::kRedshift) {
    for (const auto& [table_name, column_name] : sequence_cols) {
      std::stringstream create;
      create << "CREATE INDEX " << table_name << "_seq ON " << table_name
             << " using btree (" << column_name << ")";
      nanodbc::execute(db, create.str());
    }
  }

  txn.commit();
}

void DatasetAdmin::LoadFromS3(nanodbc::connection& db, DBType dbtype,
                              const std::string& bucket,
                              const std::string& iam_role) const {
  nanodbc::transaction txn(db);
  const std::string dataset_name = state_->config_["name"].as<std::string>();
  for (const auto& table : state_->config_["tables"]) {
    const std::string table_name = table["name"].as<std::string>();

    if (dbtype == DBType::kRDSPostgreSQL) {
      std::stringstream cmd;
      cmd << "SELECT aws_s3.table_import_from_s3(";
      cmd << "'" << table_name << "',";
      cmd << "'',";
      cmd << "'DELIMITER ''|''',";
      cmd << "aws_commons.create_s3_uri('" << bucket << "', '" << dataset_name
          << "/sf" << PaddedScaleFactor(scale_factor_) << "/" << table_name
          << ".tbl', 'us-east-1')";
      cmd << ");";
      nanodbc::execute(db, cmd.str());

    } else if (dbtype == DBType::kRedshift) {
      std::stringstream cmd;
      cmd << "COPY " << table_name;
      cmd << " FROM 's3://" << bucket << "/" << dataset_name << "/sf"
          << PaddedScaleFactor(scale_factor_) << "/" << table_name << ".tbl'";
      cmd << " IAM_ROLE '" << iam_role << "'";
      cmd << " REGION 'us-east-1'";
      nanodbc::execute(db, cmd.str());

    } else {
      throw std::runtime_error("Unsupported DBType.");
    }
  }
  txn.commit();
}

void DatasetAdmin::ResetToGenerated(nanodbc::connection& db,
                                    DBType dbtype) const {
  nanodbc::transaction txn(db);
  for (const auto& table : state_->config_["tables"]) {
    const std::string table_name = table["name"].as<std::string>();
    const uint64_t multiplier = table["multiplier"].as<uint64_t>();
    const uint64_t num_rows = multiplier * scale_factor_;
    std::optional<std::string>
        pkey_column;  // TODO: Assumes a single primary key column.

    // Find the primary key column.
    for (const auto& col : table["columns"]) {
      if (col["dist"]["type"].as<std::string>() == "primary_key") {
        pkey_column = col["name"].as<std::string>();
      }
      break;
    }

    if (!pkey_column.has_value()) {
      throw std::runtime_error("Table missing primary key: " + table_name);
    }

    std::stringstream query;
    query << "DELETE FROM " << table_name << " WHERE " << *pkey_column << " > "
          << num_rows;
    nanodbc::execute(db, query.str());
  }
  txn.commit();

  ResetSequences(db, dbtype);
}

void DatasetAdmin::ResetSequences(nanodbc::connection& db,
                                  DBType dbtype) const {
  if (dbtype == DBType::kRedshift) return;

  // Makes sure that newly inserted rows have a `seq` greater than all
  // previous rows. This change is used to extract new rows.
  // TODO: This can be made less sloppy.
  for (const auto& table : state_->config_["tables"]) {
    const std::string table_name = table["name"].as<std::string>();
    const uint64_t multiplier = table["multiplier"].as<uint64_t>();
    const uint64_t num_rows = multiplier * scale_factor_;

    for (const auto& col : table["columns"]) {
      const std::string col_name = col["name"].as<std::string>();
      if (col["dist"]["type"].as<std::string>() != "sequence") continue;

      std::stringstream reset;
      reset << "ALTER SEQUENCE " << table_name << "_" << col_name
            << "_seq RESTART WITH ";
      reset << (num_rows + 1);
      nanodbc::execute(db, reset.str());
    }
  }
}

}  // namespace cirrus
