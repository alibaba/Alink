/* Copyright 2019 The flink-ai-extended Authors. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

#include <pybind11/pybind11.h>
#include "java_file.h"
#include "rw_test.h"
namespace py = pybind11;

PYBIND11_MODULE(java_file_c, m) {
    m.doc() = "java queue file library.";

    /*
    py::class_<CrossLangQueueWriter>(m, "CrossLangQueueWriter")
       .def(py::init<>([](long buf) {return new CrossLangQueueWriter(buf); } ))
       .def("writeInt", &CrossLangQueueWriter::writeInt)
       .def("writeString", &CrossLangQueueWriter::writeString)
       .def("writeBytes", &CrossLangQueueWriter::writeBytes)
       .def("close", &CrossLangQueueWriter::close);

    py::class_<CrossLangQueueReader>(m, "CrossLangQueueReader")
       .def(py::init<>([](long buf) {return new CrossLangQueueReader(buf); } ))
       .def("readInt", &CrossLangQueueReader::readInt)
       .def("readBytes", &CrossLangQueueReader::readBytes)
       .def("readString", &CrossLangQueueReader::readString);
//       .def("read", &CrossLangQueue::Reader::read, py::return_value_policy::reference);
    */

    py::class_<JavaFile>(m, "JavaFile")
       .def(py::init<>([](std::string read_file_name, std::string write_file_name) {return new JavaFile(read_file_name, write_file_name); } ))
       .def("readBytes", &JavaFile::readBytes)
       .def("writeBytes", &JavaFile::writeBytes);

    py::class_<RWTest>(m, "RWTest")
            .def(py::init<>([]() {return new RWTest(); } ))
            .def("read", &RWTest::read)
            .def("write", &RWTest::write);
}
