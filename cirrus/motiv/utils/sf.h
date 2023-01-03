#pragma once

#include <iomanip>
#include <sstream>
#include <string>

namespace cirrus {

inline std::string PaddedScaleFactor(uint32_t sf) {
  std::stringstream builder;
  builder << std::setfill('0') << std::setw(3) << sf;
  return builder.str();
}

}  // namespace cirrus
