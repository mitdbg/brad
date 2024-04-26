#pragma once

#include <arrow/flight/sql/types.h>

namespace brad {

/// \brief Gets the mapping from SQL info ids to SqlInfoResult instances.
/// \return the cache.
arrow::flight::sql::SqlInfoResultMap GetSqlInfoResultMap();

}  // namespace brad
