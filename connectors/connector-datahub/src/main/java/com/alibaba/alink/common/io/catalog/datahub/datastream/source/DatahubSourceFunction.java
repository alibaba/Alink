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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;

import com.alibaba.alink.common.io.catalog.datahub.common.reader.RecordReader;
import com.alibaba.alink.common.io.catalog.datahub.common.source.AbstractDynamicParallelSource;
import com.alibaba.alink.common.io.catalog.datahub.common.source.SourceUtils;
import com.alibaba.alink.common.io.catalog.datahub.datastream.util.DatahubClientProvider;
import com.aliyun.datahub.client.model.GetTopicResult;
import com.aliyun.datahub.client.model.ListShardResult;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.ShardEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * DatahubSourceFunction.
 */
public class DatahubSourceFunction extends AbstractDynamicParallelSource <List<RecordEntry>, Long> {
	private static Logger logger = LoggerFactory.getLogger(DatahubSourceFunction.class);
	private String endpoint;
	private String projectName;
	private String topicName;
	private String accessId = null;
	private String accessKey = null;
	private Configuration properties;
	private transient DatahubClientProvider clientProvider;

	private Date startTime;
	private long startInMs;
	private long stopInMs;
	//private transient DatahubClient client;
	private transient GetTopicResult topic;

	private long maxRetryTimes;
	private long retryIntervalMs;
	private int batchReadSize;
	private List<ShardEntry> initShardsList;

	private transient List<String> shardIds = null;

	/**
	 * ak mode.
	 * another mode is roleArn mode.
	 */
	public DatahubSourceFunction(
			String endpoint,
			String projectName,
			String topicName,
			String accessId,
			String accessKey,
			long startInMs,
			long stopInMs) throws Exception {
		this(
				endpoint,
				projectName,
				topicName,
				accessId,
				accessKey,
				startInMs,
				stopInMs,
				20,
				1000,
				1000);
	}

	public DatahubSourceFunction(
			String endpoint,
			String projectName,
			String topicName,
			String accessId,
			String accessKey,
			long startInMs,
			long stopInMs,
			long maxRetryTimes,
			long retryIntervalMs,
			int batchReadSize) throws Exception {
		this.endpoint = endpoint;
		this.projectName = projectName;
		this.topicName = topicName;
		this.accessId = accessId;
		this.accessKey = accessKey;
		this.startInMs = startInMs;
		this.stopInMs = stopInMs;
		this.maxRetryTimes = maxRetryTimes;
		this.retryIntervalMs = retryIntervalMs;
		this.batchReadSize = batchReadSize;
		clientProvider = createProvider();

		this.startTime = new Date(this.startInMs);
	}

	public DatahubSourceFunction(
			String endpoint,
			String projectName,
			String topicName,
			Configuration properties,
			long startInMs,
			long stopInMs,
			long maxRetryTimes,
			long retryIntervalMs,
			int batchReadSize) throws Exception {
		this.endpoint = endpoint;
		this.projectName = projectName;
		this.topicName = topicName;

		this.properties = properties;

		this.startInMs = startInMs;
		this.stopInMs = stopInMs;
		this.maxRetryTimes = maxRetryTimes;
		this.retryIntervalMs = retryIntervalMs;
		this.batchReadSize = batchReadSize;
		clientProvider = createProvider();

		this.startTime = new Date(this.startInMs);
	}

	@Override
	public RecordReader <List<RecordEntry>, Long> createReader(Configuration config) throws IOException {

		if (null == topic){
			if (null == clientProvider){
				clientProvider = createProvider();
			}
			this.topic = this.clientProvider.getClient().getTopic(this.projectName, this.topicName);
		}
		initShardsList();
		return new DatahubRecordReader(this.clientProvider, this.topic, maxRetryTimes, retryIntervalMs,
									batchReadSize, initShardsList, stopInMs);
	}

	@Override
	public InputSplit[] createInputSplitsForCurrentSubTask(
			int numberOfParallelSubTasks,
			int indexOfThisSubTask) throws IOException {
		if (null == topic){
			if (null == clientProvider){
				clientProvider = createProvider();
			}
			this.topic = this.clientProvider.getClient().getTopic(this.projectName, this.topicName);
		}
		if (shardIds == null) {
			initShardsList();
			shardIds = new ArrayList<>();
			for (ShardEntry shardEntry : initShardsList) {
				shardIds.add(shardEntry.getShardId());
			}
		}

		int totalPartitionCount = initShardsList.size();
		List<Integer> subscribedShardIndexList =
				SourceUtils.modAssign("datahub" + topic.getTopicName(), numberOfParallelSubTasks, indexOfThisSubTask,
									totalPartitionCount);
		DatahubShardInputSplit[] inputSplits = new DatahubShardInputSplit[subscribedShardIndexList.size()];
		int i = 0;
		for (Integer shardIndex : subscribedShardIndexList) {
			inputSplits[i++] = new DatahubShardInputSplit(shardIndex, shardIds.get(shardIndex), this.startTime);
		}
		return inputSplits;
	}

	@Override
	public List<Tuple2<InputSplit, Long>> reAssignInputSplitsForCurrentSubTask(
			int numberOfParallelSubTasks,
			int indexOfThisSubTask,
			List<InnerProgress <Long>> allSplitsInState) throws IOException {
		initShardsList();
		List<Tuple2<InputSplit, Long>> initialProgess = new ArrayList<>();
		List<ShardEntry> subscribedPartitions = modAssign(numberOfParallelSubTasks, indexOfThisSubTask);
		for (ShardEntry shard:subscribedPartitions) {
			boolean existBefore = false;
			for (InnerProgress<Long> progress: allSplitsInState) {
				DatahubShardInputSplit split = (DatahubShardInputSplit) progress.getInputSplit();
				if (Integer.parseInt(shard.getShardId()) == Integer.parseInt(split.getShardId())){
					logger.info("Current subTask " + indexOfThisSubTask +
								" split in state:  " + progress.getCursor());
					initialProgess.add(new Tuple2<>(progress.getInputSplit(), progress.getCursor()));
					existBefore = true;
					break;
				}
			}
			if (!existBefore) {
				// 新增加的shardId 标识0为shard的开头
				logger.info("Current subTask " + indexOfThisSubTask +
							" new split: " +
							new DatahubShardInputSplit(Integer.parseInt(shard.getShardId()), shard.getShardId(), this.startTime));
				initialProgess.add(Tuple2.of(new DatahubShardInputSplit(Integer.parseInt(shard.getShardId()), shard
						.getShardId(), this.startTime), 0L));
			}
		}

		return initialProgess;
	}

	@Override
	public List<String> getPartitionList() throws Exception {
		List<String> shardIds = new ArrayList<String>();
		if (null == clientProvider){
			clientProvider = createProvider();
		}
		ListShardResult shardResult = clientProvider.getClient().listShard(this.projectName, this.topicName);
		for (ShardEntry shardEntry : shardResult.getShards()) {
			shardIds.add(String.valueOf(shardEntry.getShardId()));
		}
		Collections.sort(shardIds);
		return shardIds;
	}

	private void initShardsList() {
		if (null == clientProvider){
			clientProvider = createProvider();
		}
		if (null == initShardsList || initShardsList.size() == 0) {
			initShardsList = clientProvider.getClient().listShard(this.projectName, this.topicName).getShards();
			initShardsList.sort(Comparator.comparingInt(o -> Integer.parseInt(o.getShardId())));
		}
	}

	/**
	 * used to distinguish parse way: blob or schema mod.
	 *
	 * @return
	 */
	public GetTopicResult getTopic() {
		if (null == topic){
			if (null == clientProvider){
				clientProvider = createProvider();
			}
			this.topic = this.clientProvider.getClient().getTopic(this.projectName, this.topicName);
		}
		return this.topic;
	}

	@Override
	public String toString() {
		return String.format("%s:%s:%s", getClass().getSimpleName(), this.projectName, this.topicName);
	}

	private DatahubClientProvider createProvider(){
		if (null != accessId && null != accessKey && !accessId.isEmpty() && !accessKey.isEmpty()){
			return new DatahubClientProvider(endpoint, accessId, accessKey);
		}
		else {
			return new DatahubClientProvider(endpoint, properties);
		}
	}

	private List<ShardEntry> modAssign(
			int consumerCount,
			int consumerIndex) {
		List<ShardEntry> assignedShards = new LinkedList<>();

		for (ShardEntry shard: initShardsList) {
			if (Integer.parseInt(shard.getShardId()) % consumerCount == consumerIndex) {
				assignedShards.add(shard);
			}
		}
		return assignedShards;
	}
}
