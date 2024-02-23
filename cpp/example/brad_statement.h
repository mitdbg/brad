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

#include <arrow/flight/sql/column_metadata.h>
#include <arrow/type_fwd.h>

namespace arrow {
namespace flight {
namespace sql {
namespace brad {

/// \brief Create an object ColumnMetadata using the column type and
///        table name.
/// \param column_type  The BRAD type.
/// \param table        The table name.
/// \return             A Column Metadata object.
ColumnMetadata GetColumnMetadata(int column_type, const char* table);

class BradStatement {
 public:
  /// \brief Creates a BRAD statement.
  /// \param[in] sql       SQL statement.
  /// \return              A BRAD object.
  static arrow::Result<std::shared_ptr<BradStatement>> Create(
    const std::string& sql);

  ~BradStatement();

  /// \brief Creates an Arrow Schema based on the results of this statement.
  /// \return              The resulting Schema.
  arrow::Result<std::shared_ptr<Schema>> GetSchema() const;

  arrow::Result<std::shared_ptr<RecordBatch>> FetchResult();

  std::string* GetBradStmt() const;

 private:
  std::string* stmt_;

  BradStatement(std::string* stmt) : stmt_(stmt) {}
};

}  // namespace brad
}  // namespace sql
}  // namespace flight
}  // namespace arrow
