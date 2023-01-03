#pragma once

#include <cstdint>

namespace cirrus {

// Used to notify of a sale.
struct NotifySalesInsert {
  uint64_t s_id;
  uint64_t s_datetime;
  uint64_t s_i_id;
  uint64_t s_quantity;
  uint64_t s_price;
  uint64_t s_phys_id;  // The physical version (name is a slight misnomer).
};

// Used to notify an update.
struct NotifyInventoryUpdate {
  uint64_t i_id;
  uint64_t i_stock;
  uint64_t i_phys_id;  // The physical version (name is a slight misnomer).
};

}  // namespace cirrus
