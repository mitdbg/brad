#include "write_logger.h"

#include <iostream>

namespace cirrus {

CirrusWriteLogger::CirrusWriteLogger(const std::filesystem::path& out_dir)
    : inventory_out_(out_dir / "inventory.csv"),
      sales_out_(out_dir / "sales.csv") {
  inventory_out_ << "i_id,i_stock" << std::endl;
  sales_out_ << "s_datetime" << std::endl;
}

std::string CirrusWriteLogger::GetVersion() const { return ""; }

void CirrusWriteLogger::NotifyUpdateInventory(NotifyInventoryUpdate inventory) {
  inventory_out_ << inventory.i_id << "," << inventory.i_stock << std::endl;
}

void CirrusWriteLogger::NotifyInsertSales(NotifySalesInsert sales) {
  sales_out_ << sales.s_datetime << std::endl;
}

size_t CirrusWriteLogger::RunReportingQuery(uint64_t datetime_start,
                                            uint64_t datetime_end) {
  return 0;
}

size_t CirrusWriteLogger::RunStockFeatureQuery() { return 0; }

}  // namespace cirrus
