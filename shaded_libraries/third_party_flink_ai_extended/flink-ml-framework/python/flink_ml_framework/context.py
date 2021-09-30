# Copyright 2019 The flink-ai-extended Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =============================================================================

from time import sleep
import json
import os
from flink_ml_framework import node_pb2
from flink_ml_framework import node_service_pb2_grpc


class Context(object):

    def __str__(self):
        return self.context_pb.__str__()

    def __init__(self, context, channel):
        self.mode = context.mode
        self.roleName = context.roleName
        self.index = context.index
        self.roleParallelism = context.roleParallelism
        self.properties = context.props
        self.context_pb = context

        self.userScript = context.userScript
        self.identity = context.identity
        self.funcName = context.funcName
        self.failNum = context.failNum

        self.outQueueName = context.outQueueName
        self.inQueueName = context.inQueueName
        self.outQueueMMapLen = context.outQueueMMapLen
        self.inQueueMMapLen = context.inQueueMMapLen

        self.channel = channel
        self.stub = node_service_pb2_grpc.NodeServiceStub(self.channel)

    def from_java(self):
        return "queue://" + str(self.inQueueName) + ":" + str(self.inQueueMMapLen)

    def to_java(self):
        return "queue://" + str(self.outQueueName) + ":" + str(self.outQueueMMapLen)

    def get_failed_num(self):
        return self.failNum

    def get_finish_workers(self):
        response = self.stub.GetFinishWorker(node_pb2.NodeSimpleRequest(code=0))
        return response.workers

    def stop_job(self):
        response = self.stub.FinishJob(node_pb2.NodeSimpleRequest(code=0))

    def get_property(self, key):
        return self.properties[key]

    def get_role_parallelism_map(self):
        return self.roleParallelism

    def get_index(self):
        return self.index

    def get_role_name(self):
        return self.roleName

    def get_context_proto(self):
        return self.context_pb
