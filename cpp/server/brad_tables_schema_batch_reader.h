#pragma once

#include <memory>
#include <string>

#include "brad_statement.h"
#include "brad_statement_batch_reader.h"
#include <arrow/record_batch.h>

namespace brad {

class BradTablesWithSchemaBatchReader : public arrow::RecordBatchReader {
 private:
  std::shared_ptr<BradStatementBatchReader> reader_;
  std::string main_query_;
  bool already_executed_;

 public:
  /// Constructor for BradTablesWithSchemaBatchReader class
  /// \param reader an shared_ptr from a BradStatementBatchReader.
  /// \param main_query  SQL query that originated reader's data.
  BradTablesWithSchemaBatchReader(
      std::shared_ptr<BradStatementBatchReader> reader,
      std::string main_query)
      : reader_(std::move(reader)), main_query_(std::move(main_query)) {}

  std::shared_ptr<arrow::Schema> schema() const override;

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override;
};

}  // namespace brad
