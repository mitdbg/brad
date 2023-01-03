#pragma once

#include <nanodbc/nanodbc.h>

#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>

namespace cirrus {

// inventory(i_id, i_name, i_category, i_price, i_stock)
// sales(s_id, s_datetime, s_i_id, s_quantity, s_price)
class StoreDataset {
 public:
  StoreDataset(uint32_t scale_factor);

  void CreateTables(nanodbc::connection& connection,
                    bool for_postgres_extraction);
  void DropAll(nanodbc::connection& connection);
  void DropWorkloadGeneratedRecords(nanodbc::connection& connection);
  // Only relevant for workloads that run on PostgreSQL.
  void ResetPhysIdSequence(nanodbc::connection& connection);

  void GenerateDataFiles(std::filesystem::path out, uint32_t seed = 42);
  void GenerateAndLoad(nanodbc::connection& connection, uint32_t seed = 42);

  // Used to initialize the workers.
  void UpdateMaxStats(nanodbc::connection& connection);
  uint64_t MaxDatetime() const;
  uint64_t MaxId() const;

 private:
  using Inventory =
      std::tuple<uint64_t, std::string, uint64_t, uint64_t, uint64_t, uint64_t>;
  using InventoryCallback = std::function<void(Inventory)>;

  using Sales =
      std::tuple<uint64_t, uint64_t, uint64_t, uint64_t, uint64_t, uint64_t>;
  using SalesCallback = std::function<void(Sales)>;

  static void GenerateData(uint32_t scale_factor, uint32_t seed,
                           const InventoryCallback& handle_inventory,
                           const SalesCallback& handle_sales);

  static uint64_t SalesBaseCardinality(uint32_t scale_factor);
  static uint64_t InventoryBaseCardinality(uint32_t scale_factor);

  uint32_t scale_factor_;

  uint64_t max_s_datetime_;
  uint64_t max_i_id_;
};

}  // namespace cirrus
