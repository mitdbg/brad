#pragma once

#include <gflags/gflags.h>
#include <nanodbc/nanodbc.h>

#include <string>

#include "dbtype.h"

// The data source name specified in ~/.odbc.ini.
DECLARE_string(odbc_dsn);

// The username to use to connect.
DECLARE_string(user);

// The environment variable that stores the user's password. We use an
// environment variable to avoid passing the password in plaintext as a command
// line argument.
DECLARE_string(pwdvar);

namespace utils {

// Establishes a connection to the database specified by the gflags. This is a
// convenience function used to simplify gflags-based connection setup.
nanodbc::connection GetConnection();

}  // namespace utils
