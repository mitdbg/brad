#include <pybind11/pybind11.h>

#include <iostream>

#include "../server/brad_server.cc"

namespace py = pybind11;

// namespace brad {
//   class BradFlightSqlServer {
//    public:
//     void Create() { std::cout << "Created BradFlightSqlServer\n"; }
//   };
// }

PYBIND11_MODULE(pybind_brad_server, m) {
  // py::class_<brad::BradFlightSqlServer>(m, "BradFlightSqlServer")
  //     .def(py::init<>())
  //     .def("Create", &brad::BradFlightSqlServer::Create);

  m.doc() = "BradFlightSqlServer Python bindings";

  py::class_<brad::BradFlightSqlServer> bradServer(m, "BradFlightSqlServer");

  bradServer
      .def(py::init<std::shared_ptr<brad::BradFlightSqlServer::Impl>>())
      .def("create", &brad::BradFlightSqlServer::Create);
      // .def("get_flight_info_statement",
      //      &brad::BradFlightSqlServer::GetFlightInfoStatement)
      // .def("do_get_statement", &brad::BradFlightSqlServer::DoGetStatement);

  py::class_<brad::BradFlightSqlServer::Impl>(bradServer, "Impl")
      .def(py::init<>());
      // .def("get_flight_info_statement",
      //      &brad::BradFlightSqlServer::Impl::GetFlightInfoStatement);
      // .def("do_get_statement",
      //      &brad::BradFlightSqlServer::Impl::DoGetStatement);
}
