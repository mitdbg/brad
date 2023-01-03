#pragma once

#include <cstdint>
#include <cstring>

#include "ycsbr/types.h"

namespace ycsbr {
namespace gen {

// Chooses values from a 0-based dense range.
// Used to select existing keys for read/update/scan operations.
class Chooser {
 public:
  virtual ~Chooser() = default;
  virtual size_t Next(PRNG& prng) = 0;
  virtual void SetItemCount(size_t item_count) = 0;
  virtual void IncreaseItemCountBy(size_t delta) = 0;
};

}  // namespace gen
}  // namespace ycsbr
