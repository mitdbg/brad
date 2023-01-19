#include <chrono>
#include <iostream>
#include <sstream>
#include <thread>

#include "cirrus/config.h"
#include "inventory_wide.h"
#include "utils/sf.h"

namespace {

static const std::string kGetMaxSequenceId =
    "SELECT MAX(i_seq) FROM inventory_wide";

}

namespace cirrus {

InvETL::InvETL(uint32_t scale_factor, std::chrono::milliseconds period,
               nanodbc::connection source, std::shared_ptr<Cirrus> cirrus,
               std::shared_ptr<BenchmarkState> state)
    : WorkloadBase(std::move(state)),
      num_runs_(0),
      scale_factor_(scale_factor),
      synced_phys_id_(0),
      period_(period),
      sequence_number_(0),
      source_(std::move(source)),
      cirrus_(std::move(cirrus)) {
  Start();
}

uint64_t InvETL::NumRuns() const { return num_runs_; }

void InvETL::RunImpl() {
  synced_phys_id_ = cirrus_->GetMaxSyncedInv();
  WarmedUpAndReadyToRun();

  run_next_ = std::chrono::steady_clock::now() + period_;

  while (KeepRunning()) {
    std::this_thread::sleep_until(run_next_);
    if (!KeepRunning()) break;

    if (false /* verbose */) {
      std::cerr << "> Starting ETL sync from " << synced_phys_id_ << std::endl;
    }

    // Run the ETL.
    // TODO: This might need to be tuned.
    const auto start = std::chrono::steady_clock::now();

    uint64_t new_max_to_sync;
    {
      auto result = nanodbc::execute(source_, kGetMaxSequenceId);
      result.next();
      new_max_to_sync = result.get<uint64_t>(0);
    }

    const std::string extract = GenerateExtractQuery(sequence_number_, new_max_to_sync);
    nanodbc::execute(source_, extract);
    const auto extract_done = std::chrono::steady_clock::now();
    const auto extract_elapsed = extract_done - start;
    if (false /* verbose */) {
      std::cerr << "> Extract phase done "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                       extract_elapsed)
                       .count()
                << " ms" << std::endl;
    }
    cirrus_->RunETLSync(sequence_number_);
    const auto import_elapsed = std::chrono::steady_clock::now() - extract_done;
    if (false /* verbose */) {
      std::cerr << "> Import phase done "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                       import_elapsed)
                       .count()
                << " ms" << std::endl;
    }
    synced_phys_id_ = new_max_to_sync;
    const auto end = std::chrono::steady_clock::now();

    ++sequence_number_;
    ++num_runs_;
    AddLatency(end - start);

    // To maintain freshness under `period_`, we assume that writes continue to
    // happen during the ETL. Thus the next time the ETL should run is `start +
    // period_` (we need to include the amount of time that has elapsed since
    // the extract phase began).
    run_next_ = start + period_;
    const auto now = std::chrono::steady_clock::now();
    if (now > run_next_) {
      std::cerr << "WARNING: ETL running longer than its restart interval."
                << std::endl;
    }
  }
}

std::string InvETL::GenerateExtractQuery(uint64_t seq, uint64_t max_seq) const {
  std::stringstream builder;
  builder << "SELECT * from aws_s3.query_export_to_s3(";
  builder << "'SELECT * FROM inventory_wide";
  builder << " WHERE i_seq > " << synced_phys_id_ << " AND i_seq <= " << max_seq
          << "'";
  builder << ", aws_commons.create_s3_uri('geoffxy-research', 'etl/invwide-"
          << seq
          << ".tbl', 'us-east-1'), options :='FORMAT text, DELIMITER ''|''');";
  return builder.str();
}

}  // namespace cirrus
