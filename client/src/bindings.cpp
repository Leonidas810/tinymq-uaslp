#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h> // Necesario para std::function
#include "client.h"

namespace py = pybind11;

PYBIND11_MODULE(tinymq_module, m) {
    py::class_<tinymq::client::Client>(m, "Client")
        .def(py::init<const std::string&, const std::string&, uint16_t>(),
             py::arg("client_id"), py::arg("host") = "localhost", py::arg("port") = 1505)
        .def("connect", &tinymq::client::Client::connect)
        .def("disconnect", &tinymq::client::Client::disconnect)
        .def("subscribe", &tinymq::client::Client::subscribe)
        .def("unsubscribe", &tinymq::client::Client::unsubscribe)
        .def("publish", py::overload_cast<const std::string&, const std::vector<uint8_t>&>(
                            &tinymq::client::Client::publish))
        .def("publish", py::overload_cast<const std::string&, const std::string&>(
                            &tinymq::client::Client::publish))
        .def("is_connected", &tinymq::client::Client::is_connected)
        .def("poll", &tinymq::client::Client::poll);
}
