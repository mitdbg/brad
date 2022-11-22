#include <chrono>
#include <iostream>
#include <sstream>
#include <thread>

#include "store.h"
#include "utils/config.h"
#include "utils/sf.h"

SalesETL::SalesETL(uint32_t scale_factor, std::chrono::milliseconds period,
                   nanodbc::connection source, nanodbc::connection dest,
                   std::shared_ptr<BenchmarkState> state)
    : WorkloadBase(std::move(state)),
      num_runs_(0),
      scale_factor_(scale_factor),
      synced_phys_id_(0),
      period_(period),
      sequence_number_(0),
      source_(std::move(source)),
      dest_(std::move(dest)) {
  Start();
}

uint64_t SalesETL::NumRuns() const { return num_runs_; }

void SalesETL::RunImpl() {
  synced_phys_id_ = GetMaxSynced();
  WarmedUpAndReadyToRun();

  run_next_ = std::chrono::steady_clock::now() + period_;

  while (KeepRunning()) {
    std::this_thread::sleep_until(run_next_);
    if (!KeepRunning()) break;

    if (FLAGS_verbose) {
      std::cerr << "> Starting ETL sync from " << synced_phys_id_ << std::endl;
    }

    // Run the ETL.
    // TODO: This might need to be tuned.
    GetState()->MarkETLStart();
    const auto start = std::chrono::steady_clock::now();
    const std::string extract = GenerateExtractQuery(sequence_number_);
    nanodbc::execute(source_, extract);
    const auto extract_done = std::chrono::steady_clock::now();
    const auto extract_elapsed = extract_done - start;
    if (FLAGS_verbose) {
      std::cerr << "> Extract phase done "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                       extract_elapsed)
                       .count()
                << " ms" << std::endl;
    }
    const std::string import = GenerateImportQuery(sequence_number_);
    nanodbc::execute(dest_, import);
    const auto import_elapsed = std::chrono::steady_clock::now() - extract_done;
    if (FLAGS_verbose) {
      std::cerr << "> Import phase done "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                       import_elapsed)
                       .count()
                << " ms" << std::endl;
    }
    // TODO: Probably not a good idea to run vacuum/analyze on each load.
    // nanodbc::execute(dest_, "VACUUM;");
    // if (FLAGS_verbose) {
    //  std::cerr << "> Vacuum done" << std::endl;
    //}
    // nanodbc::execute(dest_, "ANALYZE;");
    // if (FLAGS_verbose) {
    //  std::cerr << "> Analyze done" << std::endl;
    //}
    synced_phys_id_ = GetMaxSynced();
    const auto end = std::chrono::steady_clock::now();
    GetState()->MarkETLFinish();

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

uint64_t SalesETL::GetMaxSynced() const {
  // `s_phys_id` is a monotonically increasing sequence (PostgreSQL specific
  // concept).
  auto result = nanodbc::execute(dest_, "SELECT MAX(s_phys_id) FROM sales_" +
                                            PaddedScaleFactor(scale_factor_));
  result.next();
  return result.get<uint64_t>(0);
}

std::string SalesETL::GenerateExtractQuery(uint64_t seq) const {
  std::stringstream builder;
  builder << "SELECT * from aws_s3.query_export_to_s3(";
  builder << "'SELECT * FROM sales_" << PaddedScaleFactor(scale_factor_);
  builder << " WHERE s_phys_id > " << synced_phys_id_ << "'";
  builder << ", aws_commons.create_s3_uri('geoffxy-research', 'etl/store-"
          << seq
          << ".tbl', 'us-east-1'), options :='FORMAT text, DELIMITER ''|''');";
  return builder.str();
}

std::string SalesETL::GenerateImportQuery(uint64_t seq) const {
  std::stringstream builder;
  builder << "COPY sales_" << PaddedScaleFactor(scale_factor_)
          << " FROM 's3://geoffxy-research/etl/store-" << seq
          << ".tbl' IAM_ROLE '" << FLAGS_redshift_iam_role
          << "' REGION 'us-east-1'";
  return builder.str();
}
