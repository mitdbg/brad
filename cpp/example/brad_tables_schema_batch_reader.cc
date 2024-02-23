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

#include "brad_tables_schema_batch_reader.h"

#include <sstream>

#include <arrow/array/builder_binary.h>
#include <arrow/flight/sql/column_metadata.h>
#include "arrow/flight/sql/server.h"
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>

namespace arrow {
namespace flight {
namespace sql {
namespace brad {

std::shared_ptr<Schema> BradTablesWithSchemaBatchReader::schema() const {
  return SqlSchema::GetTablesSchemaWithIncludedSchema();
}

Status BradTablesWithSchemaBatchReader::ReadNext(
  std::shared_ptr<RecordBatch>* batch) {
  std::shared_ptr<RecordBatch> first_batch;

  ARROW_RETURN_NOT_OK(reader_->ReadNext(&first_batch));

  if (!first_batch) {
      *batch = NULLPTR;
      return Status::OK();
  }

  *batch = first_batch;

  return Status::OK();
}

}  // namespace brad
}  // namespace sql
}  // namespace flight
}  // namespace arrow
