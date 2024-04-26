#pragma once

#include <memory>

#include "brad_statement.h"
#include <arrow/record_batch.h>

namespace brad {

class BradStatementBatchReader : public arrow::RecordBatchReader {
 public:
  /// \brief Creates a RecordBatchReader backed by a BRAD statement.
  /// \param[in] statement    BRAD statement to be read.
  /// \return                 A BradStatementBatchReader.
  static arrow::Result<std::shared_ptr<BradStatementBatchReader>> Create(
      const std::shared_ptr<BradStatement>& statement);

  /// \brief Creates a RecordBatchReader backed by a BRAD statement.
  /// \param[in] statement    BRAD statement to be read.
  /// \param[in] schema       Schema to be used on results.
  /// \return                 A BradStatementBatchReader..
  static arrow::Result<std::shared_ptr<BradStatementBatchReader>> Create(
      const std::shared_ptr<BradStatement>& statement,
      const std::shared_ptr<arrow::Schema>& schema);

  std::shared_ptr<arrow::Schema> schema() const override;

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override;

 private:
  std::shared_ptr<BradStatement> statement_;
  std::shared_ptr<arrow::Schema> schema_;
  bool already_executed_;

  BradStatementBatchReader(std::shared_ptr<BradStatement> statement,
                           std::shared_ptr<arrow::Schema> schema);
};

}  // namespace brad
