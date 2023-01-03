#include "cirrus/odbc.h"

namespace cirrus {

nanodbc::connection GetOdbcConnection(const CirrusConfig& config,
                                      DBType dbtype) {
  return nanodbc::connection(config.odbc_dsn(dbtype), config.odbc_user(dbtype),
                             config.odbc_pwd(dbtype));
}

}  // namespace cirrus
