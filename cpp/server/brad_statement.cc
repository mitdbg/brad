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

#include <pybind11/pybind11.h>

namespace py = pybind11;
using namespace pybind11::literals;

namespace brad {

using arrow::internal::checked_cast;

arrow::Result<std::shared_ptr<BradStatement>> BradStatement::Create(
  const std::string& sql) {
  std::string sql_statement = sql;
  std::shared_ptr<BradStatement> result(new BradStatement(&sql_statement));
  return result;
}

arrow::Result<std::shared_ptr<BradStatement>> BradStatement::Create(
  std::vector<std::vector<std::any>> query_result) {
  std::shared_ptr<BradStatement> result(
    new BradStatement(query_result));
  return result;
}

BradStatement::BradStatement(std::vector<std::vector<std::any>> query_result) {
    query_result_ = query_result;
}

BradStatement::~BradStatement() {
}

arrow::Result<std::shared_ptr<arrow::Schema>> BradStatement::GetSchema() const {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  const std::vector<std::any> &row = query_result_[0];

  for (const auto &field : row) {
    std::string field_type = field.type().name();
    if (field_type == "i") {
      fields.push_back(arrow::field("INT FIELD", arrow::int8()));
    } else if (field_type == "f") {
      fields.push_back(arrow::field("FLOAT FIELD", arrow::float16()));
    } else {
      fields.push_back(arrow::field("STRING FIELD", arrow::utf8()));
    }
  }

  return arrow::schema(fields);
}

std::string* BradStatement::GetBradStmt() const { return stmt_; }

}  // namespace brad
