#pragma once

#include <nanodbc/nanodbc.h>

#include <cstdint>
#include <memory>
#include <random>
#include <thread>
#include <utility>

#include "state.h"
#include "utils/latency_manager.h"
#include "workload_base.h"

class SalesReporting : public WorkloadBase {
 public:
  SalesReporting(uint32_t scale_factor, uint64_t num_warmup, uint32_t client_id,
                 nanodbc::connection connection,
                 std::shared_ptr<BenchmarkState> state);
  virtual ~SalesReporting() = default;

  uint64_t NumReportsRun() const;

 private:
  virtual void RunImpl() override;
  std::string GenerateQuery(uint32_t repetitions) const;
  std::pair<uint64_t, uint64_t> GenerateDatetimeRange() const;
  uint64_t GetMaxDatetime() const;

  uint64_t num_warmup_;
  uint64_t max_datetime_;
  uint64_t num_reports_run_;
  uint32_t scale_factor_;

  mutable nanodbc::connection connection_;
  mutable std::mt19937 prng_;
};

class MakeSale : public WorkloadBase {
 public:
  MakeSale(uint32_t scale_factor, uint64_t num_warmup, uint32_t client_id,
           nanodbc::connection connection,
           std::shared_ptr<BenchmarkState> state);
  virtual ~MakeSale() = default;

  uint64_t NumTxnsRun() const;
  uint64_t NumAborts() const;

 private:
  virtual void RunImpl() override;
  uint64_t GetMaxItemId() const;
  uint64_t GetMaxSaleDatetime() const;
  uint64_t GenerateSaleId();

  uint64_t num_warmup_;
  uint64_t num_txns_;
  uint64_t num_aborts_;
  uint32_t scale_factor_;
  uint32_t client_id_;
  uint64_t next_id_;
  uint64_t next_datetime_;

  mutable nanodbc::connection connection_;
};

// Implements an ETL of the sales table.
// This workload is hardcoded to use AWS S3 for data transfer.
class SalesETL : public WorkloadBase {
 public:
  // The ETL will run every `period` milliseconds.
  SalesETL(uint32_t scale_factor, std::chrono::milliseconds period,
           nanodbc::connection source, nanodbc::connection dest,
           std::shared_ptr<BenchmarkState> state);

  uint64_t NumRuns() const;

 private:
  uint64_t GetMaxSynced() const;
  std::string GenerateExtractQuery(uint64_t seq) const;
  std::string GenerateImportQuery(uint64_t seq) const;

  virtual void RunImpl() override;

  uint64_t num_runs_;
  uint32_t scale_factor_;
  uint64_t synced_datetime_;
  uint64_t sequence_number_;
  std::chrono::milliseconds period_;
  mutable nanodbc::connection source_, dest_;
};
