#include "connector.h"

#include "cirrus/dbtype.h"
#include "odbc.h"

namespace cirrus {

void Connector::Connect(const std::shared_ptr<CirrusConfig>& config) {
  read_store_type_ = config->read_store_type();
  write_store_type_ = config->write_store_type();

  read_store_ = GetOdbcConnection(*config, config->read_store_type());
  write_store_ = GetOdbcConnection(*config, config->write_store_type());
  write_store_writer_ = GetOdbcConnection(*config, config->write_store_type());

  if (config->write_store_type() == DBType::kRDSPostgreSQL) {
    // In our current setup, we only use this connection for analytics.
    nanodbc::execute(
        write_store_,
        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL "
        "REPEATABLE READ READ ONLY");
    nanodbc::execute(
        write_store_writer_,
        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL "
        "REPEATABLE READ READ WRITE");
  }

  if (config->read_store_type() == DBType::kRedshift) {
    // Disable result caching.
    nanodbc::execute(read_store_, "SET enable_result_cache_for_session = off;");
  }
}

}  // namespace cirrus
