#pragma once

#include <gflags/gflags.h>
#include <nanodbc/nanodbc.h>

#include <string>

#include "dbtype.h"

// The data source name specified in ~/.odbc.ini.
DECLARE_string(default_odbc_dsn);

namespace utils {

// Establishes a connection to the default database specified by the gflags.
// This is a convenience function used to simplify gflags-based connection
// setup.
nanodbc::connection GetConnection();

nanodbc::connection GetConnection(DBType dbtype);

}  // namespace utils
