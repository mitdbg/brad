#pragma once

#include <arrow/type.h>
#include <pybind11/pybind11.h>

#include <memory>

namespace brad {

// Converts a `brad.connection.schema.Schema` Python object into an
// `arrow::Schema`. The passed in `schema` must be an instance of
// `brad.connection.schema.Schema`.
//
// NOTE: The GIL must be held while running this function.
std::shared_ptr<arrow::Schema> ArrowSchemaFromBradSchema(
    const pybind11::object& schema);

}  // namespace brad
