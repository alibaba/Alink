/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.alink.common.io.catalog.datahub.datastream.source;

import org.apache.flink.core.io.InputSplit;

import java.util.Date;

/**
 * DatahubShardInputSplit.
 */
public class DatahubShardInputSplit implements InputSplit {
	private static final long serialVersionUID = 4128235102572836877L;

	private final int splitId;
	private String shardId;
	private Date startTime;

	public DatahubShardInputSplit(int splitId, String shardId, Date startTime) {
		this.splitId = splitId;
		this.shardId = shardId;
		this.startTime = startTime;
	}

	@Override
	public int getSplitNumber() {
		return splitId;
	}

	public String getShardId() {
		return shardId;
	}

	public Date getStartTime() {
		return startTime;
	}

	@Override
	public String toString() {
		return String.format("splitId:%s, shardId:%s, startTime:%s", splitId, shardId, startTime);
	}
}
