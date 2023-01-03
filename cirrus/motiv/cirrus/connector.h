#pragma once

#include <nanodbc/nanodbc.h>

#include <memory>

#include "cirrus/config.h"

namespace cirrus {

// Manages connections to the read and write stores.
class Connector {
 public:
  // Must be called to establish a connection before `read()` and `write()` can
  // be called.
  void Connect(const std::shared_ptr<CirrusConfig>& config);

  nanodbc::connection& read() { return read_store_; }
  nanodbc::connection& write() { return write_store_; }
  nanodbc::connection& write_writer() { return write_store_writer_; }

  DBType read_store_type() const { return read_store_type_; }
  DBType write_store_type() const { return write_store_type_; }

 private:
  DBType read_store_type_, write_store_type_;
  // `write_store_writer_` is used for our manual MV maintenance transactions.
  nanodbc::connection read_store_, write_store_, write_store_writer_;
};

}  // namespace cirrus
