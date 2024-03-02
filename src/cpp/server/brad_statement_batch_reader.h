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
