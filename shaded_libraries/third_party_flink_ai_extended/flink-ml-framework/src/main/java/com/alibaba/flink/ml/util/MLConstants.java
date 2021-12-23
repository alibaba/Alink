/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.ml.util;

public class MLConstants {
	public static final String SYS_PREFIX = "sys:";
	public static final String ENCODING_CLASS = SYS_PREFIX + "encoding_class";
	public static final String DECODING_CLASS = SYS_PREFIX + "decoding_class";
	public static final String RECORD_READER_CLASS = SYS_PREFIX + "record_reader_class";
	public static final String RECORD_WRITER_CLASS = SYS_PREFIX + "record_writer_class";
	public static final String DATA_BRIDGE_CLASS = SYS_PREFIX + "data_bridge_class";
	public static final String ML_RUNNER_CLASS = SYS_PREFIX + "ml_runner_class";

	public static final int END_STATUS_NORMAL = 0;
	public static final int END_STATUS_FLINK_ASK_TO_KILL = 1;
	public static final int END_STATUS_TF_FAIL = 999;
	public static final String DISPLAY_NAME_DUMMY_SINK = "Dummy sink";
	public static final String JOB_VERSION = "job_version";
	public static final String PYTHON_FILES = "python_files";
	public static final String USER_ENTRY_PYTHON_FILE = "user_entry_python_file";
	public static final String SCRIPT_RUNNER_CLASS = "script_runner_class";
	public static final String AM_STATE_CLASS = "am_state_class";
	public static final String AM_STATE_MACHINE_CLASS = "am_state_machine_class";
	public static final String PYTHON_PATH = "python_path";
	public static final String PYTHONPATH_ENV = "PYTHONPATH";
	public static final String VIRTUAL_ENV_DIR = "virtual_env_dir";
	public static final String LD_LIBRARY_PATH = "LD_LIBRARY_PATH";
	public static String HADOOP_HDFS_HOME = "HADOOP_HDFS_HOME";
	public static String LD_PRELOAD = "LD_PRELOAD";
	public static String JAVA_HOME = "JAVA_HOME";
	public static String HADOOP_CLASSPATH = "HADOOP_CLASSPATH";
	public static final String CLASSPATH = "CLASSPATH";
	public static final String CONFIG_CLUSTER_PATH = "cluster";
	public static final String CONFIG_STORAGE_TYPE = "storage_type";
	public static final String STORAGE_MEMORY = "storage_memory";
	public static final String STORAGE_ZOOKEEPER = "storage_zookeeper";
	public static final String STORAGE_CUSTOM = "storage_custom";
	public static final String STORAGE_IMPL_CLASS = "storage_impl_class";
	public static final String CONFIG_ZOOKEEPER_CONNECT_STR = "zookeeper_connect_str";
	public static final String CONFIG_ZOOKEEPER_TIMEOUT = "zookeeper_timeout";
	public static final String CONFIG_TENSORFLOW_FLINK = "tensorflow-flink.xml";
	public static final String CONFIG_ZOOKEEPER_BASE_PATH = "zookeeper_base_path";
	public static final String CROSS_QUEUE_SIZE = "cross_queue_size";
	public static final String CONFIG_EVENT_REPORTER = "event_reporter";
	public static final String CONFIG_JOB_NAME = "job_name";
	public static final String CONFIG_JOB_HAS_INPUT = "job_has_input";
	public static final String INTUT_DEFAULT_NAME = "input";
	public static long TIMEOUT = 5L * 60 * 1000;
	public static final int INT_SIZE = 4;
	public static final String WORK_DIR = "current_work_dir";
	public static final String STARTUP_SCRIPT = "startup.py";
	public static final String STARTUP_SCRIPT_FILE = "startup_file_name";
	public static final String ENV_PROPERTY_PREFIX = "ENV:";
	public static final String SYS_PROPERTY_PREFIX = "SYS:";
	public static final String REMOTE_CODE_ZIP_FILE = "remote_code_zip_file";
	public static final String CODE_DIR_NAME = "code_dir_name";
	public static final String CODE_DIR = "code_dir";
	public static final String USE_DISTRIBUTE_CACHE = "use_distribute_cache";
	public static final String PYTHON_SCRIPT_DIR = "python_script_dir";
	public static final String START_WITH_STARTUP = "SYS:start_with_startup";
	public static final String FLINK_VERTEX_NAME = "flink.vertex.name";
	public static final String CHECKPOINT_DIR = "checkpoint_dir";
	public static final String EXPORT_DIR = "export_dir";

	public static final String SERVER_RPC_CONTACT_TIMEOUT = "server.rpc.contact.timeout";
	public static final String SERVER_RPC_CONTACT_TIMEOUT_DEFAULT = "600000";

	public static final String NODE_IDLE_TIMEOUT = "node.idle.timeout";
	public static final String NODE_IDLE_TIMEOUT_DEFAULT = "600000";

	public static final String AM_REGISTRY_TIMEOUT = "am.registry.timeout";
	public static final String AM_REGISTRY_TIMEOUT_DEFAULT = String.valueOf(TIMEOUT);

	public static final String HEARTBEAT_TIMEOUT = "heartbeat.timeout";
	public static final String HEARTBEAT_TIMEOUT_DEFAULT = String.valueOf(TIMEOUT);

	public static final String SEPERATOR_COMMA = ",";
	public static final String FLINK_HOOK_CLASSNAMES = "flink_hook_class_names";

	// similar to Flink's failover strategy that controls the recovery logic for failed tasks
	// set to "all" or "individual" to restart all tasks or only failed tasks, respectively
	public static final String FAILOVER_STRATEGY = "failover.strategy";
	public static final String FAILOVER_RESTART_ALL_STRATEGY = "all";
	public static final String FAILOVER_RESTART_INDIVIDUAL_STRATEGY = "individual";
	public static final String FAILOVER_STRATEGY_DEFAULT = FAILOVER_RESTART_ALL_STRATEGY;
	public static final String PYTHON_VERSION = "python.version";

	public static final String GPU_INFO = "gpu_info";
}
