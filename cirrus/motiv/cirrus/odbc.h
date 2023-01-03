#pragma once

#include <nanodbc/nanodbc.h>

#include "cirrus/config.h"

namespace cirrus {

nanodbc::connection GetOdbcConnection(const CirrusConfig& config, DBType dbtype);

}  // namespace cirrus
