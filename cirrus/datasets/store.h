#pragma once

#include <cstdint>
#include <nanodbc/nanodbc.h>

class StoreDataset {
 public:
  StoreDataset(nanodbc::connection& connection);

  void CreateTables();
  void DropAll();
  void LoadData(uint32_t scale_factor, uint32_t seed = 42);

  void LoadSimple();

  uint64_t GetMaxDatetime() const;

 private:
  uint64_t SalesBaseCardinality(uint32_t scale_factor) const;
  uint64_t InventoryBaseCardinality(uint32_t scale_factor) const;

  nanodbc::connection& connection_;
};
