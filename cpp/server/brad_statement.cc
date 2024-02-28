// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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

BradStatement::~BradStatement() {
}

arrow::Result<std::shared_ptr<arrow::Schema>> BradStatement::GetSchema() const {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.push_back(arrow::field("Day", arrow::int8()));
  fields.push_back(arrow::field("Month", arrow::int8()));
  fields.push_back(arrow::field("Year", arrow::int16()));
  return arrow::schema(fields);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> BradStatement::FetchResult() {
  arrow::Int8Builder int8builder;
  int8_t days_raw[5] = {1, 12, 17, 23, 28};
  ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw, 5));
  std::shared_ptr<arrow::Array> days;
  ARROW_ASSIGN_OR_RAISE(days, int8builder.Finish());

  int8_t months_raw[5] = {1, 3, 5, 7, 1};
  ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw, 5));
  std::shared_ptr<arrow::Array> months;
  ARROW_ASSIGN_OR_RAISE(months, int8builder.Finish());

  arrow::Int16Builder int16builder;
  int16_t years_raw[5] = {1990, 2000, 1995, 2000, 1995};
  ARROW_RETURN_NOT_OK(int16builder.AppendValues(years_raw, 5));
  std::shared_ptr<arrow::Array> years;
  ARROW_ASSIGN_OR_RAISE(years, int16builder.Finish());

  std::shared_ptr<arrow::RecordBatch> record_batch;

  arrow::Result<std::shared_ptr<arrow::Schema>> result = GetSchema();
  if (result.ok()) {
    std::shared_ptr<arrow::Schema> schema = result.ValueOrDie();
    record_batch = arrow::RecordBatch::Make(schema,
                                            days->length(),
                                            {days, months, years});
    return record_batch;
  }

  return arrow::Status::OK();
}

std::string* BradStatement::GetBradStmt() const { return stmt_; }

}  // namespace brad
