#pragma once

#include <cstdint>
#include <random>

namespace ycsbr {
namespace gen {

using PhaseID = uint64_t;
using ProducerID = uint64_t;
using PRNG = std::mt19937;

// The workload runner reserves 16 bits for the phase ID and producer ID (helps
// us ensure inserts are always new keys.)
inline constexpr uint64_t kMaxKey = (1ULL << 48) - 1;

// PhaseIDs are 8 bit values. We reserve 0x00 (for loaded keys) and 0xFF (for
// negative lookups).
inline constexpr size_t kMaxNumPhases = (1ULL << 8) - 2;

}  // namespace gen
}  // namespace ycsbr
