#include "brad_tables_schema_batch_reader.h"

#include <sstream>

#include <arrow/array/builder_binary.h>
#include <arrow/flight/sql/column_metadata.h>
#include "arrow/flight/sql/server.h"
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>

namespace brad {

std::shared_ptr<arrow::Schema> BradTablesWithSchemaBatchReader::schema() const {
  return arrow::flight::sql::SqlSchema::GetTablesSchemaWithIncludedSchema();
}

arrow::Status BradTablesWithSchemaBatchReader::ReadNext(
  std::shared_ptr<arrow::RecordBatch>* batch) {
  if (already_executed_) {
    *batch = NULLPTR;
    return arrow::Status::OK();
  }

  std::shared_ptr<arrow::RecordBatch> first_batch;

  ARROW_RETURN_NOT_OK(reader_->ReadNext(&first_batch));

  if (!first_batch) {
      *batch = NULLPTR;
      return arrow::Status::OK();
  }

  *batch = first_batch;
  already_executed_ = true;

  return arrow::Status::OK();
}

}  // namespace brad
