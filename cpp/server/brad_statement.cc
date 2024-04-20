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
  std::vector<std::vector<std::any>> query_result) {
  std::shared_ptr<BradStatement> result(
    std::make_shared<BradStatement>(query_result));
  return result;
}

BradStatement::BradStatement(std::vector<std::vector<std::any>> query_result) :
    query_result_(std::move(query_result)) {}

BradStatement::~BradStatement() {
}

arrow::Result<std::shared_ptr<arrow::Schema>> BradStatement::GetSchema() const {
  if (schema_) {
    return schema_;
  }

  std::vector<std::shared_ptr<arrow::Field>> fields;

  if (query_result_.size() > 0) {
    const std::vector<std::any> &row = query_result_[0];

    int counter = 0;
    for (const auto &field : row) {
      std::string field_type = field.type().name();
      if (field_type == "i") {
        fields.push_back(arrow::field("INT FIELD " + std::to_string(++counter), arrow::int8()));
      } else if (field_type == "f") {
        fields.push_back(arrow::field("FLOAT FIELD " + std::to_string(++counter), arrow::float32()));
      } else {
        fields.push_back(arrow::field("STRING FIELD " + std::to_string(++counter), arrow::utf8()));
      }
    }
  }

  schema_ = arrow::schema(fields);
  return schema_;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> BradStatement::FetchResult() {
  std::shared_ptr<arrow::Schema> schema = GetSchema().ValueOrDie();

  const int num_rows = query_result_.size();

  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(schema->num_fields());

  for (int field_ix = 0; field_ix < schema->num_fields(); ++field_ix) {
    const auto &field = schema->fields()[field_ix];
    if (field->type() == arrow::int8()) {
      arrow::Int8Builder int8builder;
      int8_t values_raw[num_rows];
      for (int row_ix = 0; row_ix < num_rows; ++row_ix) {
        values_raw[row_ix] = std::any_cast<int>(query_result_[row_ix][field_ix]);
      }
      ARROW_RETURN_NOT_OK(int8builder.AppendValues(values_raw, num_rows));

      std::shared_ptr<arrow::Array> values;
      ARROW_ASSIGN_OR_RAISE(values, int8builder.Finish());

      columns.push_back(values);
    } else if (field->type() == arrow::float32()) {
      arrow::FloatBuilder floatbuilder;
      float values_raw[num_rows];
      for (int row_ix = 0; row_ix < num_rows; ++row_ix) {
        values_raw[row_ix] = std::any_cast<float>(query_result_[row_ix][field_ix]);
      }
      ARROW_RETURN_NOT_OK(floatbuilder.AppendValues(values_raw, num_rows));

      std::shared_ptr<arrow::Array> values;
      ARROW_ASSIGN_OR_RAISE(values, floatbuilder.Finish());

      columns.push_back(values);
    } else if (field->type() == arrow::utf8()) {
      arrow::StringBuilder stringbuilder;
      for (int row_ix = 0; row_ix < num_rows; ++row_ix) {
        const std::string* str = std::any_cast<const std::string>(&(query_result_[row_ix][field_ix]));
        ARROW_RETURN_NOT_OK(stringbuilder.Append(str->data(), str->size()));
      }

      std::shared_ptr<arrow::Array> values;
      ARROW_ASSIGN_OR_RAISE(values, stringbuilder.Finish());
    }
  }

  std::shared_ptr<arrow::RecordBatch> record_batch =
    arrow::RecordBatch::Make(schema,
                             num_rows,
                             columns);
  return record_batch;
}

std::string* BradStatement::GetBradStmt() const { return stmt_; }

}  // namespace brad
