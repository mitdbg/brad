#include <pybind11/pybind11.h>

#include <iostream>

namespace py = pybind11;

namespace brad {
  class BradFlightSqlServer {
   public:
    void Create() { std::cout << "Created BradFlightSqlServer\n"; }
  };
}

PYBIND11_MODULE(pybind_brad_server, m) {
  py::class_<brad::BradFlightSqlServer>(m, "BradFlightSqlServer")
      .def(py::init<>())
      .def("Create", &brad::BradFlightSqlServer::Create);
}
