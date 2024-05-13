#include "brad_statement.h"

#include <algorithm>

#include <arrow/api.h>
#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/flight/sql/column_metadata.h>
#include <arrow/scalar.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/util/checked_cast.h>

namespace brad {

using arrow::internal::checked_cast;

arrow::Result<std::shared_ptr<BradStatement>> BradStatement::Create(
  const std::string& sql) {
  std::string sql_statement = sql;
  std::shared_ptr<BradStatement> result(new BradStatement(&sql_statement));
  return result;
}

arrow::Result<std::shared_ptr<BradStatement>> BradStatement::Create(
  std::shared_ptr<arrow::RecordBatch> result_record_batch,
  std::shared_ptr<arrow::Schema> schema) {
    std::shared_ptr<BradStatement> result(
      std::make_shared<BradStatement>(result_record_batch, schema));
    return result;
}

BradStatement::BradStatement(std::shared_ptr<arrow::RecordBatch> result_record_batch,
                             std::shared_ptr<arrow::Schema> schema) :
  result_record_batch_(std::move(result_record_batch)),
  schema_(std::move(schema)) {}

BradStatement::~BradStatement() {
}

arrow::Result<std::shared_ptr<arrow::Schema>> BradStatement::GetSchema() const {
  return schema_;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> BradStatement::FetchResult() {
  return result_record_batch_;
}

std::string* BradStatement::GetBradStmt() const { return stmt_; }

}  // namespace brad
