#pragma once

#include <nanodbc/nanodbc.h>

#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>

// inventory(i_id, i_name, i_category, i_price, i_stock)
// sales(s_id, s_datetime, s_i_id, s_quantity, s_price)
class StoreDataset {
 public:
  StoreDataset(uint32_t scale_factor);

  void CreateTables(nanodbc::connection& connection);
  void DropAll(nanodbc::connection& connection);
  void DropWorkloadGeneratedRecords(nanodbc::connection& connection);

  void GenerateDataFiles(std::filesystem::path out, uint32_t seed = 42);
  void GenerateAndLoad(nanodbc::connection& connection, uint32_t seed = 42);

  uint64_t GetMaxDatetime(nanodbc::connection& connection) const;

 private:
  using Inventory =
      std::tuple<uint64_t, std::string, uint64_t, uint64_t, uint64_t>;
  using InventoryCallback = std::function<void(Inventory)>;

  using Sales = std::tuple<uint64_t, uint64_t, uint64_t, uint64_t, uint64_t>;
  using SalesCallback = std::function<void(Sales)>;

  static void GenerateData(uint32_t scale_factor, uint32_t seed,
                           const InventoryCallback& handle_inventory,
                           const SalesCallback& handle_sales);

  static uint64_t SalesBaseCardinality(uint32_t scale_factor);
  static uint64_t InventoryBaseCardinality(uint32_t scale_factor);

  uint32_t scale_factor_;
};
