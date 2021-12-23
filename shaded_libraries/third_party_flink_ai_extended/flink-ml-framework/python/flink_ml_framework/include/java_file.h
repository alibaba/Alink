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

#ifndef FLINK_ML_FRAMEWORK_QUEUE_FILE_H
#define FLINK_ML_FRAMEWORK_QUEUE_FILE_H

#include <pybind11/pybind11.h>
#include "spscqueue.h"

namespace py = pybind11;

class JavaFile{


public:
    JavaFile(const std::string &read_file_name, const std::string &write_file_name) : read_file_name(read_file_name),
                                                                                      write_file_name(
                                                                                              write_file_name) {
        long buf_r;
        std::string fileName_r;
        bool res_r = ParseQueuePath(read_file_name, fileName_r, &buf_r);
        if(fileName_r.empty()){
            reader = new SPSCQueueInputStream(buf_r);
        } else{
            reader = new SPSCQueueInputStream(fileName_r.c_str(), buf_r);
        }

        long buf_w;
        std::string fileName_w;
        bool res_w = ParseQueuePath(write_file_name, fileName_w, &buf_w);
        if(fileName_w.empty()){
            writer = new SPSCQueueOutputStream(buf_w);
        } else{
            writer = new SPSCQueueOutputStream(fileName_w.c_str(), buf_w);
        }
        readBuffer = new char[8*1024*1024];
    }

    bool writeBytes(char* buf, size_t len){
        writer->writeBytes(buf, len);
        return true;
    }

    py::bytes readBytes(size_t len){
        int readSize = reader->readBytes(readBuffer, len);
        if(len == readSize){
            return py::bytes(std::string(readBuffer, len));
        } else{
            std::cerr << "read EOF" << std::endl;
            return py::bytes(std::string(""));
        }
    }

    virtual ~JavaFile() {
        if(nullptr != reader){
            reader->close();
            delete(reader);
            reader = nullptr;
        }
        if(nullptr != writer){
            writer->close();
            delete(writer);
            writer = nullptr;
        }
        delete(readBuffer);

    }

    bool ParseQueuePath(const std::string &fname, std::string& fileName, long* buf) {
        if (!buf) {
            std::cerr << "buf can not be null." << std::endl;
            return false;
        }
        std::cout << "queue name: " << fname << std::endl;
        const char* prefix = "queue://";
        if(fname.find(prefix)!=0) {
            std::cerr << "queue path doesn't start with 'queue://': " << fname << std::endl;
            return false;
        }
        std::string fname2 = fname.substr(strlen(prefix));
        std::vector<std::string> tokens;
        std::string token;
        std::istringstream tokenStream(fname2);
        while (std::getline(tokenStream, token, ':'))
        {
            tokens.push_back(token);
        }
        if(tokens.size()==1) {
            buf[0] = atol(tokens[0].c_str());
        }else if(tokens.size()==2) {
            fileName = tokens[0];
            buf[0] = atol(tokens[1].c_str());
        }else {
            std::cerr << "Invalid queue construction." << fname << std::endl;
            std::cerr.flush();
            return false;
        }
        std::cout << "fileName: " << fileName << std::endl;
        std::cout << "buf:" << *buf << std::endl;
        std::cout.flush();
        return true;
    }

private:
    SPSCQueueInputStream* reader;
    SPSCQueueOutputStream* writer;
    std::string read_file_name;
    std::string write_file_name;
    char* readBuffer;
};
#endif //FLINK_ML_FRAMEWORK_QUEUE_FILE_H
