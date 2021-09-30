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

from pyflink.java_gateway import get_gateway


class MLCONSTANTS(object):
    ml_constants = get_gateway().jvm.com.alibaba.flink.ml.util.MLConstants

    SYS_PREFIX = str(ml_constants.SYS_PREFIX)
    ENCODING_CLASS = str(ml_constants.ENCODING_CLASS)
    DECODING_CLASS = str(ml_constants.DECODING_CLASS)
    RECORD_READER_CLASS = str(ml_constants.RECORD_READER_CLASS)
    RECORD_WRITER_CLASS = str(ml_constants.RECORD_WRITER_CLASS)
    DATA_BRIDGE_CLASS = str(ml_constants.DATA_BRIDGE_CLASS)
    ML_RUNNER_CLASS = str(ml_constants.ML_RUNNER_CLASS)

    END_STATUS_NORMAL = str(ml_constants.END_STATUS_NORMAL)
    END_STATUS_FLINK_ASK_TO_KILL = str(ml_constants.END_STATUS_FLINK_ASK_TO_KILL)
    END_STATUS_TF_FAIL = str(ml_constants.END_STATUS_TF_FAIL)
    DISPLAY_NAME_DUMMY_SINK = str(ml_constants.DISPLAY_NAME_DUMMY_SINK)
    JOB_VERSION = str(ml_constants.JOB_VERSION)
    PYTHON_FILES = str(ml_constants.PYTHON_FILES)
    USER_ENTRY_PYTHON_FILE = str(ml_constants.USER_ENTRY_PYTHON_FILE)
    SCRIPT_RUNNER_CLASS = str(ml_constants.SCRIPT_RUNNER_CLASS)
    AM_STATE_CLASS = str(ml_constants.AM_STATE_CLASS)
    AM_STATE_MACHINE_CLASS = str(ml_constants.AM_STATE_MACHINE_CLASS)
    PYTHON_PATH = str(ml_constants.PYTHON_PATH)
    PYTHONPATH_ENV = str(ml_constants.PYTHONPATH_ENV)
    VIRTUAL_ENV_DIR = str(ml_constants.VIRTUAL_ENV_DIR)
    LD_LIBRARY_PATH = str(ml_constants.LD_LIBRARY_PATH)
    HADOOP_HDFS_HOME = str(ml_constants.HADOOP_HDFS_HOME)
    LD_PRELOAD = str(ml_constants.LD_PRELOAD)
    JAVA_HOME = str(ml_constants.JAVA_HOME)
    HADOOP_CLASSPATH = str(ml_constants.HADOOP_CLASSPATH)
    CLASSPATH = str(ml_constants.CLASSPATH)
    CONFIG_CLUSTER_PATH = str(ml_constants.CONFIG_CLUSTER_PATH)
    CONFIG_STORAGE_TYPE = str(ml_constants.CONFIG_STORAGE_TYPE)
    STORAGE_MEMORY = str(ml_constants.STORAGE_MEMORY)
    STORAGE_ZOOKEEPER = str(ml_constants.STORAGE_ZOOKEEPER)
    STORAGE_CUSTOM = str(ml_constants.STORAGE_CUSTOM)
    STORAGE_IMPL_CLASS = str(ml_constants.STORAGE_IMPL_CLASS)
    CONFIG_ZOOKEEPER_CONNECT_STR = str(ml_constants.CONFIG_ZOOKEEPER_CONNECT_STR)
    CONFIG_ZOOKEEPER_TIMEOUT = str(ml_constants.CONFIG_ZOOKEEPER_TIMEOUT)
    CONFIG_TENSORFLOW_FLINK = str(ml_constants.CONFIG_TENSORFLOW_FLINK)
    CONFIG_ZOOKEEPER_BASE_PATH = str(ml_constants.CONFIG_ZOOKEEPER_BASE_PATH)
    CROSS_QUEUE_SIZE = str(ml_constants.CROSS_QUEUE_SIZE)
    CONFIG_EVENT_REPORTER = str(ml_constants.CONFIG_EVENT_REPORTER)
    CONFIG_JOB_NAME = str(ml_constants.CONFIG_JOB_NAME)
    CONFIG_JOB_HAS_INPUT = str(ml_constants.CONFIG_JOB_HAS_INPUT)
    INTUT_DEFAULT_NAME = str(ml_constants.INTUT_DEFAULT_NAME)
    TIMEOUT = str(ml_constants.TIMEOUT)
    INT_SIZE = str(ml_constants.INT_SIZE)
    WORK_DIR = str(ml_constants.WORK_DIR)
    STARTUP_SCRIPT = str(ml_constants.STARTUP_SCRIPT)
    STARTUP_SCRIPT_FILE = str(ml_constants.STARTUP_SCRIPT_FILE)
    ENV_PROPERTY_PREFIX = str(ml_constants.ENV_PROPERTY_PREFIX)
    SYS_PROPERTY_PREFIX = str(ml_constants.SYS_PROPERTY_PREFIX)
    REMOTE_CODE_ZIP_FILE = str(ml_constants.REMOTE_CODE_ZIP_FILE)
    CODE_DIR_NAME = str(ml_constants.CODE_DIR_NAME)
    CODE_DIR = str(ml_constants.CODE_DIR)
    USE_DISTRIBUTE_CACHE = str(ml_constants.USE_DISTRIBUTE_CACHE)
    PYTHON_SCRIPT_DIR = str(ml_constants.PYTHON_SCRIPT_DIR)
    START_WITH_STARTUP = str(ml_constants.START_WITH_STARTUP)
    FLINK_VERTEX_NAME = str(ml_constants.FLINK_VERTEX_NAME)
    CHECKPOINT_DIR = str(ml_constants.CHECKPOINT_DIR)
    EXPORT_DIR = str(ml_constants.EXPORT_DIR)

    SERVER_RPC_CONTACT_TIMEOUT = str(ml_constants.SERVER_RPC_CONTACT_TIMEOUT)
    SERVER_RPC_CONTACT_TIMEOUT_DEFAULT = str(ml_constants.SERVER_RPC_CONTACT_TIMEOUT_DEFAULT)

    NODE_IDLE_TIMEOUT = str(ml_constants.NODE_IDLE_TIMEOUT)
    NODE_IDLE_TIMEOUT_DEFAULT = str(ml_constants.NODE_IDLE_TIMEOUT_DEFAULT)

    AM_REGISTRY_TIMEOUT = str(ml_constants.AM_REGISTRY_TIMEOUT)
    AM_REGISTRY_TIMEOUT_DEFAULT = str(ml_constants.AM_REGISTRY_TIMEOUT_DEFAULT)

    HEARTBEAT_TIMEOUT = str(ml_constants.HEARTBEAT_TIMEOUT)
    HEARTBEAT_TIMEOUT_DEFAULT = str(ml_constants.HEARTBEAT_TIMEOUT_DEFAULT)

    SEPERATOR_COMMA = str(ml_constants.SEPERATOR_COMMA)
    FLINK_HOOK_CLASSNAMES = str(ml_constants.FLINK_HOOK_CLASSNAMES)

    FAILOVER_STRATEGY = str(ml_constants.FAILOVER_STRATEGY)
    FAILOVER_RESTART_ALL_STRATEGY = str(ml_constants.FAILOVER_RESTART_ALL_STRATEGY)
    FAILOVER_RESTART_INDIVIDUAL_STRATEGY = str(ml_constants.FAILOVER_RESTART_INDIVIDUAL_STRATEGY)
    FAILOVER_STRATEGY_DEFAULT = str(ml_constants.FAILOVER_STRATEGY_DEFAULT)

    PYTHON_VERSION = str(ml_constants.PYTHON_VERSION)

    GPU_INFO = str(ml_constants.GPU_INFO)