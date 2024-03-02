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
