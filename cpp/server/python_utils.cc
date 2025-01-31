#include "python_utils.h"

#include <arrow/type.h>

#include <iostream>
#include <vector>

namespace py = pybind11;

namespace {

std::shared_ptr<arrow::DataType> ArrowDataTypeFromBradDataType(
    const pybind11::object& data_type) {
  // NOTE: If you change values here, make sure to change
  // `brad.connection.schema.DataType` as well.
  const int64_t value = py::cast<int64_t>(data_type.attr("value"));
  switch (value) {
    // DataType.Integer
    case 1:
      return arrow::int64();

    // DataType.Float
    case 2:
      return arrow::float32();

    // DataType.Decimal
    case 3:
      // Ideally these values should be stored with the data type and not be
      // hardcoded here.
      return arrow::decimal(/*precision=*/10, /*scale=*/2);

    // DataType.String
    case 4:
      return arrow::utf8();

    // DataType.Timestamp
    case 5:
      return arrow::date64();

    default:
    case 0:
      return arrow::null();
  }
}

}  // namespace

namespace brad {

std::shared_ptr<arrow::Schema> ArrowSchemaFromBradSchema(
    const pybind11::object& schema) {
  const size_t num_fields = py::cast<size_t>(schema.attr("num_fields"));
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(num_fields);

  for (const auto& brad_field : schema) {
    std::string field_name = py::cast<std::string>(brad_field.attr("name"));
    std::shared_ptr<arrow::DataType> data_type =
        ArrowDataTypeFromBradDataType(brad_field.attr("data_type"));
    fields.push_back(arrow::field(std::move(field_name), std::move(data_type)));
  }

  return arrow::schema(std::move(fields));
}

}  // namespace brad
