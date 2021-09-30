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

#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/lib/strings/stringprintf.h"
#include "tensorflow/core/platform/mutex.h"
#include "tensorflow/core/platform/types.h"
#include "tensorflow/core/framework/resource_mgr.h"
#include "tensorflow/core/framework/resource_op_kernel.h"
#include "tensorflow/core/platform/logging.h"
#include "tensorflow/core/framework/op.h"
#include "tensorflow/core/framework/shape_inference.h"
#include "tensorflow/core/lib/io/record_writer.h"

using namespace tensorflow;
namespace tensorflow {

    class FlinkWriterInterface: public ResourceBase{
    public:
        virtual string DebugString() const override {
            return string("WriterBase");
        }
        virtual Status Write(const OpInputList& values) = 0;
        virtual Status Close() = 0;
        virtual ~FlinkWriterInterface() = default;
    };

    class FlinkTFRecordWriter : public FlinkWriterInterface {
    public:
        FlinkTFRecordWriter(const string& node_name, std::string address)
                : adderss_(address), name_(node_name) {
            VLOG(0)  << "FlinkTFRecordWriter:" << name_ << ":" << address << std::endl;
            Status s = Env::Default()->NewWritableFile(address, &file_);
            TF_CHECK_OK(s);
            writer_.reset(new io::RecordWriter(file_.get()));
        }

        ~FlinkTFRecordWriter() override {
        }

        Status Write(const OpInputList& values) override {
            if(1 != values.size()){
                return errors::Internal("FlinkTFRecordWriter only support 1 tensor!");
            }
            int64 elementNum = values[0].NumElements();
            for (int i = 0; i < elementNum; i++){
                string v = values[0].flat<tstring>()(i);
                writer_->WriteRecord(v);
            }

            return Status::OK();
        }

        Status Close() override {
            writer_->Close();
            writer_.reset(nullptr);
            file_->Close();
            file_.reset(nullptr);
            return Status::OK();
        }

    private:
        std::string adderss_;
        std::string name_;
        std::unique_ptr<WritableFile> file_;
        std::unique_ptr<io::RecordWriter> writer_;
    };


    class FlinkTFRecordWriterOp : public OpKernel {
    public:
        explicit FlinkTFRecordWriterOp(OpKernelConstruction* context)
                : OpKernel(context), have_handle_(false) {
            OP_REQUIRES_OK(context, context->allocate_persistent(
                    tensorflow::DT_STRING,
                    tensorflow::TensorShape({2}), &handle_, nullptr));
            OP_REQUIRES_OK(context, context->GetAttr("address", &address));
        }

        ~FlinkTFRecordWriterOp() override {
            if (have_handle_ && cinfo_.resource_is_private_to_kernel()) {
                TF_CHECK_OK(cinfo_.resource_manager()->Delete<FlinkTFRecordWriter>(
                        cinfo_.container(), cinfo_.name()));
            }
        }

        void Compute(OpKernelContext* context) override {
            mutex_lock l(mu_);
            if (!have_handle_) {
                OP_REQUIRES_OK(context, cinfo_.Init(context->resource_manager(), def(), false));
                FlinkTFRecordWriter* writer = nullptr;
                OP_REQUIRES_OK(context,
                               cinfo_.resource_manager()->LookupOrCreate<FlinkTFRecordWriter>(
                                       cinfo_.container(), cinfo_.name(), &writer,
                                       [this](FlinkTFRecordWriter ** ret) {
                                           *ret = new FlinkTFRecordWriter(name(), address);
                                           return Status::OK();
                                       }));
                writer->Unref();
                auto h = handle_.AccessTensor(context)->flat<tstring>();
                h(0) = cinfo_.container();
                h(1) = cinfo_.name();
                have_handle_ = true;
            }
            context->set_output_ref(0, &mu_, handle_.AccessTensor(context));
        }

    private:
        mutex mu_;
        bool have_handle_ TF_GUARDED_BY(mu_);
        PersistentTensor handle_ TF_GUARDED_BY(mu_);
        ContainerInfo cinfo_;
        string address;
    };


    class FlinkTFRecordWriteOp : public OpKernel {
    public:
        using OpKernel::OpKernel;
        explicit FlinkTFRecordWriteOp(OpKernelConstruction* ctx)
                : OpKernel(ctx) {
        }

        void Compute(OpKernelContext* context) override {
            FlinkTFRecordWriter* writer = NULL;
            OP_REQUIRES_OK(context,
                           GetResourceFromContext(context, "writer_handle", &writer));
            OpInputList values;
            OP_REQUIRES_OK(context, context->input_list("values", &values));
            OP_REQUIRES_OK(context, writer->Write(values));
            writer->Unref();
        }

    };


    class FlinkTFRecordCloseOp : public OpKernel {
    public:
        using OpKernel::OpKernel;
        explicit FlinkTFRecordCloseOp(OpKernelConstruction* ctx)
                : OpKernel(ctx) {
        }

        void Compute(OpKernelContext* context) override {
            FlinkTFRecordWriter* writer = NULL;
            OP_REQUIRES_OK(context,
                           GetResourceFromContext(context, "writer_handle", &writer));

            OP_REQUIRES_OK(context, writer->Close());
            writer->Unref();
        }
    };


}  // namespace tensorflow
using tensorflow::shape_inference::DimensionHandle;
using tensorflow::shape_inference::InferenceContext;
using tensorflow::shape_inference::ShapeHandle;

REGISTER_OP("FlinkRecordWriter")
        .Attr("address: string=''")
        .Output("writer_handle: Ref(string)")
        .Attr("container: string = ''")
        .Attr("shared_name: string = ''")
        .SetIsStateful()
        .SetShapeFn(shape_inference::ScalarShape)
        .Doc(R"doc(
A writer that write Row tensor to blink service.
address: string the flink queue buffer handle
writer_handle: Handle to the writer.
)doc");

REGISTER_OP("FlinkRecordWrite")
        .Attr("VALUE_TYPE: list({string})")
        .Input("writer_handle: Ref(string)")
        .Input("values: VALUE_TYPE")
        .SetShapeFn([](InferenceContext* c) {
            std::vector<ShapeHandle> input_handles;
            c->input("values", &input_handles);
            if(1 == input_handles.size()) {
                return Status::OK();
            } else{
                return errors::Internal("flink queue write only one tensor");
            }
        })
        .Doc(R"doc(
Write a tensor elements to flink queue given by writer_handle.
writer_handle: Handle to the writer.
values: Values to write.
)doc");

REGISTER_OP("FlinkRecordClose")
        .Input("writer_handle: Ref(string)")
        .SetShapeFn([](InferenceContext* c) {
            return Status::OK();
        })
        .Doc(R"doc(
    Close the flink queue writer.
)doc");

REGISTER_KERNEL_BUILDER(Name("FlinkRecordWriter").Device(DEVICE_CPU), FlinkTFRecordWriterOp);
REGISTER_KERNEL_BUILDER(Name("FlinkRecordWrite").Device(DEVICE_CPU), FlinkTFRecordWriteOp);
REGISTER_KERNEL_BUILDER(Name("FlinkRecordClose").Device(DEVICE_CPU), FlinkTFRecordCloseOp);

