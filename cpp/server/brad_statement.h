#pragma once

#include <memory>
#include <string>

#include <arrow/flight/sql/column_metadata.h>
#include <arrow/type_fwd.h>

namespace brad {

/// \brief Create an object ColumnMetadata using the column type and
///        table name.
/// \param column_type  The BRAD type.
/// \param table        The table name.
/// \return             A Column Metadata object.
// ColumnMetadata GetColumnMetadata(int column_type, const char* table);

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
  arrow::Result<std::shared_ptr<arrow::Schema>> GetSchema() const;

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> FetchResult();

  std::string* GetBradStmt() const;

 private:
  std::string* stmt_;

  BradStatement(std::string* stmt) : stmt_(stmt) {}
};

}  // namespace brad