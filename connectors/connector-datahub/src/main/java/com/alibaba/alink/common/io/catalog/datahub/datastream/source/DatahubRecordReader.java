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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.core.io.InputSplit;

import com.alibaba.alink.common.io.catalog.datahub.common.errorcode.ConnectorErrors;
import com.alibaba.alink.common.io.catalog.datahub.common.reader.AbstractPartitionNumsListener;
import com.alibaba.alink.common.io.catalog.datahub.common.reader.RecordReader;
import com.alibaba.alink.common.io.catalog.datahub.common.exception.ErrorUtils;
import com.alibaba.alink.common.io.catalog.datahub.common.reader.Interruptible;
import com.alibaba.alink.common.io.catalog.datahub.datastream.util.DatahubClientProvider;
import com.aliyun.datahub.client.model.CursorType;
import com.aliyun.datahub.client.model.GetCursorResult;
import com.aliyun.datahub.client.model.GetRecordsResult;
import com.aliyun.datahub.client.model.GetTopicResult;
import com.aliyun.datahub.client.model.ListShardResult;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.ShardEntry;
import com.aliyun.datahub.client.model.ShardState;
import com.aliyun.datahub.exception.InvalidOperationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * DatahubRecordReader.
 */
public class DatahubRecordReader extends AbstractPartitionNumsListener
		implements RecordReader <List<RecordEntry>, Long>, Interruptible {
	private static final Logger LOG = LoggerFactory.getLogger(DatahubRecordReader.class);

	private DatahubShardInputSplit currentSplit;
	private DatahubClientProvider clientProvider;
	private GetTopicResult topic;

	private String cursor;
	private Long sequence;

	private String shardId;

	private volatile boolean interrupted = false;

	private long maxRetryTimes = 20;
	private long retryIntervalMs = 1000;
	private int batchReadSize = 10;

	private long lastReadTime = 0;

	private volatile List<RecordEntry> currentRecords = new ArrayList<>();
	private volatile long currentWatermark = Long.MIN_VALUE;
	private volatile long currentMessageTimestamp = Long.MIN_VALUE;
	private long dataFetchedDelay = 0;

	private long stopInMs = Long.MAX_VALUE;

	private String stopCursor = null;

	private List<ShardEntry> initShardList;
	private ShardEntry shard;
	private ShardState shardState;
	private boolean readFinished = false;

	public DatahubRecordReader(
			DatahubClientProvider client,
			GetTopicResult topic,
			long maxRetryTimes,
			long retryIntervalMs,
			int batchReadSize,
			List<ShardEntry> initShardList,
			long stopInMs) {
		this.clientProvider = client;
		this.topic = topic;
		this.maxRetryTimes = maxRetryTimes;
		this.retryIntervalMs = retryIntervalMs;
		this.batchReadSize = batchReadSize;
		this.initShardList = initShardList;
		setInitPartitionCount(initShardList.size());
		this.stopInMs = stopInMs;
	}

	@Override
	public void interrupt() {
		interrupted = true;
	}

	@Override
	public long getWatermark() {
		return this.currentWatermark;
	}

	@Override
	public void open(InputSplit split, RuntimeContext context) throws IOException {
		this.currentSplit = (DatahubShardInputSplit) split;
		this.shardId = this.currentSplit.getShardId();
		for (ShardEntry shardEntry:initShardList){
			if (shardId.equalsIgnoreCase(shardEntry.getShardId())){
				shard = shardEntry;
				shardState = shard.getState();
				break;
			}
		}

		GetCursorResult cursorResult = this.clientProvider.getClient().getCursor(
				this.topic.getProjectName(), this.topic.getTopicName(),
				this.shardId, CursorType.SYSTEM_TIME, this.currentSplit.getStartTime().getTime());

		this.cursor = cursorResult.getCursor();
		this.sequence = cursorResult.getSequence();

		if (stopInMs != Long.MAX_VALUE) {
			cursorResult = this.clientProvider.getClient().getCursor(
					this.topic.getProjectName(), this.topic.getTopicName(),
					this.shardId, CursorType.SYSTEM_TIME, stopInMs);
			this.stopCursor = cursorResult.getCursor();
		}
		LOG.info("Open " + split + ": sequence-" + sequence + " using startInMs" +
			this.currentSplit.getStartTime().getTime()
				+ " Cursor:" + cursor);

		initPartitionNumsListener();
	}

	@Override
	public int getPartitionsNums() {
		List<String> shardIds = new ArrayList<>();

		ListShardResult shardResult = clientProvider
			.getClient()
			.listShard(this.topic.getProjectName(), this.topic.getTopicName());

		for (ShardEntry shardEntry : shardResult.getShards()) {
			shardIds.add(String.valueOf(shardEntry.getShardId()));
		}
		return shardIds.size();
	}

	@Override
	public String getReaderName() {
		return "DataHub-" + topic.getProjectName() + ":" + topic.getTopicName();
	}

	@Override
	public boolean next() throws IOException, InterruptedException {

		GetRecordsResult getRecordsResult;

		Exception holedException;

		while (true) {
			holedException = null;
			int retryTimes = 0;
			while (retryTimes++ < this.maxRetryTimes) {
				try {
					holedException = null;
					if (isPartitionChanged()) {
						LOG.info("Source datahub partitions has changed during the read progress");
						ErrorUtils.throwException(ConnectorErrors.INST.sourcePartitionChangeFailOverRecoryError(getReaderName(),
								String.valueOf(initPartitionCount), String.valueOf(getPartitionsNums())));
					}
					if (interrupted) {
						LOG.info("Received interrupt command, finish this reader.");
						return false;
					}
					if (readFinished) {
						this.sequence = Long.MIN_VALUE;
						LOG.info("Read finshed this shard, finish this reader." + shard.getShardId() + " " + shard.getState());
						return false;
					}

					if (stopCursor != null && stopCursor.equals(cursor)) {
						LOG.info("Reach stop cursor, finish this reader");
						return false;
					}

					long startTime = System.currentTimeMillis();
					getRecordsResult = this.clientProvider.getClient().getRecords(
							this.topic.getProjectName(),
							this.topic.getTopicName(),
							this.shardId,
							this.topic.getRecordSchema(),
							this.cursor,
							this.batchReadSize);
					long endTime = System.currentTimeMillis();
					if (getRecordsResult != null) {
						this.cursor = getRecordsResult.getNextCursor();
						if (getRecordsResult.getRecords() != null && !getRecordsResult.getRecords().isEmpty()) {
							currentRecords = getRecordsResult.getRecords();
							long oldestTimeStamp = Long.MAX_VALUE;
							for (RecordEntry recordEntry:currentRecords) {
								if (recordEntry.getSystemTime() < oldestTimeStamp) {
									oldestTimeStamp = recordEntry.getSystemTime();
								}
							}
							this.currentMessageTimestamp = oldestTimeStamp;
							this.currentWatermark = oldestTimeStamp;
							dataFetchedDelay = System.currentTimeMillis() - oldestTimeStamp;

							if (currentWatermark >= stopInMs) {
								LOG.info("Oldest record of the data with timestamp({}), stop timestamp({})",
										currentWatermark, stopInMs);
								return false;
							}

							this.sequence = getRecordsResult.getStartSequence() + currentRecords.size();

							if (System.currentTimeMillis() - lastReadTime >= 60000) {
								lastReadTime = System.currentTimeMillis();
								LOG.info("call next cost time in ms:[{}]", (endTime - startTime));
							}
							return true;
						}
					}
					break;
				} catch (InvalidOperationException e) {
					if (e.getMessage().contains("The specified shard is not active")) {
						LOG.warn("Finish Read This Shard", e);
						readFinished = true;
					}
				} catch (Exception e) {
					clientProvider.getClient(true, true);
					LOG.warn("failed to read from datahub, try again", e);
					Thread.sleep(this.retryIntervalMs);
					holedException = e;
				}
			}
			try {
				Thread.sleep(100);
			} catch (Exception ignored) {}
			if (holedException != null) {
				ErrorUtils.throwException(ConnectorErrors.INST.datahubReadDataError(holedException.getMessage()),
										holedException);
			}
		}
	}

	@Override
	public List<RecordEntry> getMessage(){
		return this.currentRecords;
	}

	@Override
	public void close() throws IOException {
		destroyPartitionNumsListener();
	}

	@Override
	public void seek(Long seq) throws IOException {
		this.sequence = seq;
		LOG.info("Seek shardId:" + String.valueOf(shardId) + ": sequence-" + sequence);
		try {
			if (seq == 0L) {
				this.cursor = this.clientProvider.getClient().getCursor(
						this.topic.getProjectName(),
						this.topic.getTopicName(),
						this.shardId,
						CursorType.OLDEST
				).getCursor();
			} else if (seq == Long.MIN_VALUE) {
				readFinished = true;
			} else {
				this.cursor = this.clientProvider.getClient().getCursor(
						this.topic.getProjectName(),
						this.topic.getTopicName(),
						this.shardId,
						CursorType.SEQUENCE,
						this.sequence).getCursor();
			}
		} catch (InvalidOperationException e){
			LOG.error("Sequence: " + sequence, e);
			if (e.getMessage().contains("The specified shard is not active")
				&& shardState.equals(ShardState.CLOSED)){
				readFinished = true;
			} else {
				throw e;
			}
		}
	}

	@Override
	public Long getProgress() throws IOException {
		return this.sequence;
	}

	@Override
	public long getDelay() {
		return this.currentMessageTimestamp;
	}

	@Override
	public long getFetchedDelay() {
		return dataFetchedDelay;
	}

	@Override
	public boolean isHeartBeat() {
		return false;
	}
}
