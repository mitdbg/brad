#include "brad_statement_batch_reader.h"

#include <arrow/builder.h>
#include "brad_statement.h"

namespace brad {

std::shared_ptr<arrow::Schema> BradStatementBatchReader::schema() const {
  return schema_;
}

BradStatementBatchReader::BradStatementBatchReader(
    std::shared_ptr<BradStatement> statement,
    std::shared_ptr<arrow::Schema> schema)
    : statement_(std::move(statement)),
      schema_(std::move(schema)) {}

arrow::Result<std::shared_ptr<BradStatementBatchReader>>
BradStatementBatchReader::Create(
  const std::shared_ptr<BradStatement>& statement_) {
  ARROW_ASSIGN_OR_RAISE(auto schema, statement_->GetSchema());

  std::shared_ptr<BradStatementBatchReader> result(
      new BradStatementBatchReader(statement_, schema));

  return result;
}

arrow::Result<std::shared_ptr<BradStatementBatchReader>>
BradStatementBatchReader::Create(
  const std::shared_ptr<BradStatement>& statement,
  const std::shared_ptr<arrow::Schema>& schema) {
  std::shared_ptr<BradStatementBatchReader> result(
      new BradStatementBatchReader(statement, schema));

  return result;
}

arrow::Status BradStatementBatchReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* out) {
  if (already_executed_) {
    *out = NULLPTR;
    return arrow::Status::OK();
  }

  ARROW_ASSIGN_OR_RAISE(*out, statement_->FetchResult());
  already_executed_ = true;
  return arrow::Status::OK();
}

}  // namespace brad
