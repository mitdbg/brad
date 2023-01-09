#include "column_gen.h"

namespace cirrus {

void UniformColumnGenerator::WriteNext(std::ostream& out, PRNG& prng) {
  out << dist_(prng);
}

void IncrementColumnGenerator::WriteNext(std::ostream& out, PRNG& prng) {
  out << next_;
  ++next_;
}

void IncreasingColumnGenerator::WriteNext(std::ostream& out, PRNG& prng) {
  out << last_;
  last_ += gap_dist_(prng);
}

}  // namespace cirrus
