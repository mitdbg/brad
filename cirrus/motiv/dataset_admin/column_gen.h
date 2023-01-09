#pragma once

#include <iostream>
#include <random>

namespace cirrus {

enum class ColumnDist {
  kIncrement,
  kUniform,
  kIncreasing,
  kUniformForeignKey,
};

enum class ColumnType {
  kInt,
  kForeignKey,
};

using PRNG = std::mt19937;

class ColumnGenerator {
 public:
  virtual void WriteNext(std::ostream& out, PRNG& prng) = 0;
  virtual ColumnDist Type() const = 0;
};

class UniformColumnGenerator : public ColumnGenerator {
 public:
  UniformColumnGenerator(uint64_t min, uint64_t max) : dist_(min, max) {}
  void WriteNext(std::ostream& out, PRNG& prng) override;
  ColumnDist Type() const override { return ColumnDist::kUniform; }

 private:
  std::uniform_int_distribution<uint64_t> dist_;
};

class IncrementColumnGenerator : public ColumnGenerator {
 public:
  IncrementColumnGenerator(uint64_t start_from) : next_(start_from) {}
  void WriteNext(std::ostream& out, PRNG& prng) override;
  ColumnDist Type() const override { return ColumnDist::kIncrement; }

 private:
  uint64_t next_;
};

class IncreasingColumnGenerator : public ColumnGenerator {
 public:
  IncreasingColumnGenerator(uint64_t start_from, uint64_t max_gap)
      : last_(start_from), gap_dist_(1, max_gap) {}
  void WriteNext(std::ostream& out, PRNG& prng) override;
  ColumnDist Type() const override { return ColumnDist::kIncreasing; }

 private:
  uint64_t last_;
  std::uniform_int_distribution<uint64_t> gap_dist_;
};

class UniformForeignColumnGenerator : public ColumnGenerator {
 public:
  UniformForeignColumnGenerator(const std::vector<uint64_t>& source)
      : source_(source), index_dist_(0, source.size() - 1) {}
  void WriteNext(std::ostream& out, PRNG& prng) override;
  ColumnDist Type() const override { return ColumnDist::kUniformForeignKey; }

 private:
  const std::vector<uint64_t>& source_;
  std::uniform_int_distribution<uint64_t> index_dist_;
};

}  // namespace cirrus
