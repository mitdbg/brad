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

#include "brad_statement_batch_reader.h"

#include <arrow/builder.h>
#include "brad_statement.h"

namespace arrow {
namespace flight {
namespace sql {
namespace brad {

std::shared_ptr<Schema> BradStatementBatchReader::schema() const { return schema_; }

BradStatementBatchReader::BradStatementBatchReader(
    std::shared_ptr<BradStatement> statement, std::shared_ptr<Schema> schema)
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
  const std::shared_ptr<Schema>& schema) {
  std::shared_ptr<BradStatementBatchReader> result(
      new BradStatementBatchReader(statement, schema));

  return result;
}

Status BradStatementBatchReader::ReadNext(std::shared_ptr<RecordBatch>* out) {
  ARROW_ASSIGN_OR_RAISE(*out, statement_->FetchResult());

  return Status::OK();
}

}  // namespace brad
}  // namespace sql
}  // namespace flight
}  // namespace arrow
