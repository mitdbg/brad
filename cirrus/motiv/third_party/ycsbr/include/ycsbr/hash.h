#pragma once

#include <cstdint>

namespace ycsbr {
namespace gen {

inline constexpr uint64_t kFNVOffsetBasis64 = 0xCBF29CE484222325ULL;
inline constexpr uint64_t kFNVPrime64 = 1099511628211ULL;

// A fast 64-bit hash function. This implementation was adapted from YCSB.
// See: http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
inline uint64_t FNVHash64(uint64_t val) {
  uint64_t hashval = kFNVOffsetBasis64;

  for (int i = 0; i < sizeof(uint64_t); ++i) {
    uint64_t octet = val & 0xFF;
    val >>= 8;

    hashval ^= octet;
    hashval *= kFNVPrime64;
  }
  return hashval;
}

}  // namespace gen
}  // namespace ycsbr
