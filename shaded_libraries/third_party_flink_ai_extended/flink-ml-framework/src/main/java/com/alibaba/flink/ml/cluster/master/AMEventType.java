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

package com.alibaba.flink.ml.cluster.master;

/**
 * 	INTI_AM_STATE init application master event.
 * 	REGISTER_NODE register node event.
 * 	COMPLETE_CLUSTER collect all cluster node event.
 * 	FINISH_NODE cluster node finish event.
 * 	FINISH_CLUSTER all cluster node finish event.
 * 	FAIL_NODE cluster node report fail event.
 * 	FAILED_CLUSTER cluster failed event.
 * 	RESTART_CLUSTER restart cluster all node event.
 * 	STOP_JOB stop all cluster node event.
 */
public enum AMEventType {
	INTI_AM_STATE,
	REGISTER_NODE,
	COMPLETE_CLUSTER,
	FINISH_NODE,
	FINISH_CLUSTER,
	FAIL_NODE,
	FAILED_CLUSTER,
	RESTART_CLUSTER,
	STOP_JOB
}
