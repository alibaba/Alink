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

from __future__ import print_function
import sys
import importlib
import threading
import ctypes
import logging
import traceback
from flink_ml_framework import context
import grpc
from flink_ml_framework import node_pb2
from flink_ml_framework import node_service_pb2_grpc


def parse_dir_script(script_path):
    if sys.platform.startswith("win"):
        index = str(script_path).rindex('\\')
    else:
        index = str(script_path).rindex('/')
    dir_str = script_path[0: index + 1]
    script_name = script_path[index + 1: len(script_path) - 3]
    return dir_str, script_name


def start_user_func(function, ml_context):
    try:
        function(ml_context)
    except Exception as e:
        logging.error(traceback.format_exc())
        raise


def start_user_thread(function, ml_context):
    local_t = threading.Thread(target=start_user_func, args=(function, ml_context,), name="user_thread")
    local_t.setDaemon(True)
    local_t.start()
    return local_t


def terminate_thread(thread):
    """Terminates a python thread from another thread.

    :param thread: a threading.Thread instance
    """
    if not thread.isAlive():
        return

    exc = ctypes.py_object(SystemExit)
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_long(thread.ident), exc)
    if res == 0:
        raise ValueError("nonexistent thread id")
    elif res > 1:
        # """if it returns a number greater than one, you're in trouble,
        # and you should call it again with exc=NULL to revert the effect"""
        ctypes.pythonapi.PyThreadState_SetAsyncExc(thread.ident, None)
        raise SystemError("PyThreadState_SetAsyncExc failed")

    thread.join()


def createContext(node_address):
    channel = grpc.insecure_channel(node_address)
    stub = node_service_pb2_grpc.NodeServiceStub(channel)
    response = stub.GetContext(node_pb2.ContextRequest(message=''))
    context_proto = response.context
    return context.Context(context_proto, channel)


if __name__ == "__main__":
    assert len(sys.argv) == 2, 'Invalid cmd line argument ' + str(sys.argv)

    print ('Running user func in process mode')
    sys.stdout.flush()

    address = sys.argv[1]

    context = createContext(address)

    # setup default logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [' + context.identity + '-python-%(filename)s:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        )

    print ("########## " + context.userScript)
    script_str = context.userScript
    key = context.identity
    func_name = context.funcName
    dir_name = parse_dir_script(script_str)
    sys.path.insert(0, dir_name[0])
    user_py = importlib.import_module(dir_name[1])
    func = getattr(user_py, func_name)
    logging.info(key + ' calling user func ' + func_name)
    func(context)
    logging.info(key + " python run finish")
