// bindings.cpp
#include <pybind11/pybind11.h>
#include "broker.h"  // Asegúrate de que la ruta al header sea la correcta

namespace py = pybind11;

PYBIND11_MODULE(tinymq_module, m) {
    m.doc() = "Módulo de Python para controlar el broker TinyMQ";
    py::class_<tinymq::Broker>(m, "Broker")
        .def(py::init<uint16_t, size_t>(),
             py::arg("port") = 1505,
             py::arg("thread_pool_size") = 4,
             "Constructor del Broker TinyMQ")
        .def("start", &tinymq::Broker::start, "Inicia el broker")
        .def("stop", &tinymq::Broker::stop, "Detiene el broker");
}

