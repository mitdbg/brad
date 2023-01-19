#pragma once

#include <string>

#include "cirrus/cirrus.h"
#include "cirrus/config.h"
#include "cirrus/table_version.h"
#include "connector.h"
#include "utils/thread_pool.h"

namespace cirrus {

class CirrusImpl : public Cirrus {
 public:
  explicit CirrusImpl(std::shared_ptr<CirrusConfig> config, Strategy strategy);
  ~CirrusImpl() = default;

  void EstablishThreadLocalConnections() override;
  std::string GetVersion() const override;

  void SetUpViews() override;

  void SyncTableVersions() override;

  void NotifyUpdateInventory(NotifyInventoryUpdate inventory) override;
  void NotifyInsertSales(NotifySalesInsert sales) override;

  size_t RunReportingQuery(uint64_t datetime_start,
                           uint64_t datetime_end) override;
  size_t RunStockFeatureQuery() override;

  uint64_t GetMaxDatetime() const override;

  void NotifyUpdateInventoryWide(NotifyInventoryUpdate inventory);
  size_t RunCategoryStockQuery() override;

  void RunETLSync(uint64_t sequence_num, uint64_t max_synced_version) override;
  uint64_t GetMaxSyncedInv() override;
  void SyncWideTableVersions() override;

 private:
  std::string GenerateReportingQuery(uint64_t datetime_start,
                                     uint64_t datetime_end) const;

  // Different query execution strategies.
  size_t StockFeatureAllOnOne();
  size_t StockFeatureLatestStream();
  size_t StockFeatureHotPlacement();

  void RunWriteStoreMVUpdate();

  // Different query execution strategies for the wide inventory dataset.
  size_t WideAllOnOne();
  size_t WideHotPlacement();
  size_t WideExtractImport();

  std::shared_ptr<CirrusConfig> config_;
  ThreadPool bg_workers_;
  Strategy strategy_;

  TableVersion sales_version_;
  TableVersion inventory_version_;

  uint64_t last_updated_sales_id_;

  static thread_local Connector connections_;
};

}  // namespace cirrus
