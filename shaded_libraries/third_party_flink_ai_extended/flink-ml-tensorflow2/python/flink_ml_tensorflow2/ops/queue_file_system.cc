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

#include "tensorflow/core/lib/io/path.h"
#include "tensorflow/core/platform/mutex.h"

#include "tensorflow/core/platform/logging.h"
#include "queue_file_system.h"
#include "tensorflow/core/lib/core/status.h"
#include "spscqueue.h"
#include <sstream>
#include <string>
#include <vector>

namespace tensorflow {

    Status ParseQueuePath(const string &fname, std::string& fileName, int64* buf) {
        if (!buf) {
            return errors::Internal("buf can not be null.");
        }
        VLOG(0) << "queue name: " << fname;
        const char* prefix = "queue://";
        if(fname.find(prefix)!=0) {
            return errors::InvalidArgument("queue path doesn't start with 'queue://': ",
                                           fname);
        }
        string fname2 = fname.substr(strlen(prefix));
        std::vector<std::string> tokens;
        std::string token;
        std::istringstream tokenStream(fname2);
        while (std::getline(tokenStream, token, ':'))
        {
           tokens.push_back(token);
        }
        if(tokens.size()==1) {
            buf[0] = static_cast<int64>(atol(tokens[0].c_str()));
        }else if(tokens.size()==2) {
            fileName = tokens[0];
            buf[0] = static_cast<int64>(atol(tokens[1].c_str()));
        }else {
            return errors::InvalidArgument("Invalid queue construction.", fname);
        }
        VLOG(0) << "fileName: " << fileName;
        VLOG(0) << "buf:" << *buf;
        return Status::OK();
    }


    class QueueRandomAccessFile : public RandomAccessFile {
    public:
        QueueRandomAccessFile(int64 buf);
        QueueRandomAccessFile(const std::string& fileName, int64 len);

        ~QueueRandomAccessFile(){
            reader->close();
            delete(reader);
        }

        Status Read(uint64 offset, size_t n, StringPiece* result,
                    char* scratch) const override {
            int readSize = reader->readBytes(scratch, n);
            *result = StringPiece(scratch, readSize);
            if(n == readSize){
                return Status::OK();
            } else{
                VLOG(0) << "read EOF";
                return Status(error::OUT_OF_RANGE, "Reach EOF");
            }

        }

    private:
        SPSCQueueInputStream* reader;
    };

    QueueRandomAccessFile::QueueRandomAccessFile(int64 buf){
        reader = new SPSCQueueInputStream(buf);
    }
    QueueRandomAccessFile::QueueRandomAccessFile(const std::string& fileName, int64 len) {
        reader = new SPSCQueueInputStream(fileName.c_str(), len);
    }


    class QueueWritableFile : public WritableFile {
    public:
        QueueWritableFile(int64 buf) {
            writer = new SPSCQueueOutputStream(buf);
        }

        QueueWritableFile(const std::string& fileName, int64 len) {
            writer = new SPSCQueueOutputStream(fileName.c_str(), len);
        }


        ~QueueWritableFile() override {
            if(nullptr != writer){
                delete writer;
            }
        }

        Status Append(StringPiece data) override {
//            VLOG(0) << "write size:" << data.size();
//            for(int i = 0; i < data.size(); i++) {
//                VLOG(0) << "write:" << (int)data.data()[i];
//            }
            writer->writeBytes(data.data(), data.size());
            return Status::OK();
        }

        Status Close() override {
            writer->close();
            VLOG(0) << "close QueueFileSystem";
            return Status::OK();
        }

        Status Flush() override {
            return Status::OK();
        }

        Status Sync() override {
            return Status::OK();
        }

    private:
        SPSCQueueOutputStream* writer;
    };


    QueueFileSystem::QueueFileSystem() {

    }

    QueueFileSystem::~QueueFileSystem() {

    }

    Status QueueFileSystem::NewRandomAccessFile(
            const string& fname, std::unique_ptr<RandomAccessFile>* result) {
        int64 buf;
        std::string fileName;
        TF_RETURN_IF_ERROR(ParseQueuePath(fname, fileName, &buf));
        if(fileName.empty())
        {
            result->reset(new QueueRandomAccessFile(buf));
        }else
        {
            result->reset(new QueueRandomAccessFile(fileName, buf));
        }
        return Status::OK();
    }

    Status QueueFileSystem::NewWritableFile(const string& fname,
                                            std::unique_ptr<WritableFile>* result) {
        int64 buf;
        std::string fileName;
        TF_RETURN_IF_ERROR(ParseQueuePath(fname, fileName, &buf));
        if(fileName.empty())
        {
            result->reset(new QueueWritableFile(buf));
        }else
        {
            result->reset(new QueueWritableFile(fileName.c_str(), buf));
        }
        return Status::OK();
    }

    Status QueueFileSystem::NewAppendableFile(const string& fname,
                                              std::unique_ptr<WritableFile>* result) {
        int64 buf;
        std::string fileName;
        TF_RETURN_IF_ERROR(ParseQueuePath(fname, fileName, &buf));
        if(fileName.empty())
        {
            result->reset(new QueueWritableFile(buf));
        }else
        {
            result->reset(new QueueWritableFile(fileName, buf));
        }
        return Status::OK();
    }

    Status QueueFileSystem::NewReadOnlyMemoryRegionFromFile(
            const string& fname, std::unique_ptr<ReadOnlyMemoryRegion>* result) {
        return errors::Unimplemented("Unimplemented NewReadOnlyMemoryRegionFromFile");

    }

    Status QueueFileSystem::FileExists(const string& fname) {
        return Status::OK();
    }

    Status QueueFileSystem::GetChildren(const string& dir,
                                        std::vector<string>* result) {
        return errors::Unimplemented("Unimplemented GetChildren");

    }

    Status QueueFileSystem::Stat(const string& fname, FileStatistics* stats) {
        return errors::Unimplemented("Unimplemented Stat");

    }

    Status QueueFileSystem::DeleteFile(const string& fname) {
        return errors::Unimplemented("Unimplemented DeleteFile");
    }

    Status QueueFileSystem::CreateDir(const string& dirname) {
        return errors::Unimplemented("Unimplemented CreateDir");
    }

    Status QueueFileSystem::DeleteDir(const string& dirname) {
        return errors::Unimplemented("Unimplemented DeleteDir");

    }

    Status QueueFileSystem::GetFileSize(const string& fname, uint64* file_size) {
        *file_size = UINT64_MAX;
        return Status::OK();
    }

    Status QueueFileSystem::RenameFile(const string& src, const string& target) {
        return errors::Unimplemented("Unimplemented RenameFile");
    }

    Status QueueFileSystem::GetMatchingPaths(const string& pattern,
                            std::vector<string>* results){
        return errors::Unimplemented("Unimplemented GetMatchingPaths");
    }
    REGISTER_FILE_SYSTEM("queue", QueueFileSystem);

}  // namespace tensorflow

