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

package com.alibaba.alink.common.io.catalog.datahub.datastream.sink;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.io.catalog.datahub.common.MetricUtils;
import com.alibaba.alink.common.io.catalog.datahub.common.MetricUtils.LatencyGauge;
import com.alibaba.alink.common.io.catalog.datahub.common.errorcode.ConnectorErrors;
import com.alibaba.alink.common.io.catalog.datahub.datastream.util.DatahubClientProvider;
import com.alibaba.alink.common.io.catalog.datahub.common.exception.ErrorUtils;
import com.alibaba.alink.common.io.catalog.datahub.common.sink.HasRetryTimeout;
import com.alibaba.alink.common.io.catalog.datahub.common.sink.Syncable;
import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.model.PutErrorEntry;
import com.aliyun.datahub.client.model.PutRecordsResult;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.ShardEntry;
import com.aliyun.datahub.client.model.ShardState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

/**
 * DatahubOutputFormat.
 * @param <T>
 */
public class DatahubOutputFormat<T> extends RichOutputFormat<T> implements Syncable, HasRetryTimeout {
	private static final long serialVersionUID = 706130977536622837L;
	private static Logger logger = LoggerFactory.getLogger(DatahubOutputFormat.class);

	private String endPoint;
	private String projectName;
	private String topicName;
	private String accessKey;
	private String accessId;

	private Configuration properties;

	private RowTypeInfo rowTypeInfo;

	private int bufferSize = 5000;
	private int batchSize = 50;
	private long batchWriteTimeout = 20000; //20s如果没有攒批如果没有触发，则主动flush数据

	private DatahubClient client;
	private int shardNum;

	private boolean isPrintRecord = false;
	private int retryTimeoutInMills = 1000;
	private int maxRetryTimes = 3;

	private long lastWriteTime = 0;
	private List<String> shardIdList = new ArrayList<String>();

	private List<RecordEntry> putBuffer = new ArrayList<>();

	private DatahubClientProvider datahubClientProvider;

	private Meter outTps;
	private Meter outBps;
	private LatencyGauge latencyGauge;
	private String timeZone;

	private transient Timer flusher;
	private transient volatile Exception flushException = null;
	private volatile boolean flushError = false;
	private DatahubRecordResolver <T> recordResolver;

	public DatahubOutputFormat(
			String endPoint,
			String projectName,
			String topicName,
			String accessId,
			String accessKey,
			RowTypeInfo rowTypeInfo) {
		this.endPoint = endPoint;
		this.projectName = projectName;
		this.topicName = topicName;
		this.accessKey = accessKey;
		this.accessId = accessId;
		this.rowTypeInfo = rowTypeInfo;
	}

	public DatahubOutputFormat(
			String endPoint,
			String projectName,
			String topicName,
			Configuration properties,
			RowTypeInfo rowTypeInfo) {
		this.endPoint = endPoint;
		this.projectName = projectName;
		this.topicName = topicName;
		this.properties = properties;
		this.rowTypeInfo = rowTypeInfo;
	}

	/**
	 * Start flusher that will flush buffer automatically.
	 */
	protected void scheduleFlusher() {
		flusher = new Timer("DatahubOutputFormat.buffer.flusher");
		flusher.schedule(new TimerTask() {
			@Override
			public void run() {
				try {
					if (System.currentTimeMillis() - lastWriteTime >= batchWriteTimeout){
						synchronized (this){
							sync();
						}
					}
				} catch (Exception e) {
					logger.error("flush buffer to Datahub failed", e);
					flushException = e;
					flushError = true;
				}
			}
		}, batchWriteTimeout, batchWriteTimeout);
	}

	public DatahubOutputFormat<T> setRecordResolver(
		DatahubRecordResolver <T> recordResolver) {
		this.recordResolver = recordResolver;
		return this;
	}

	public DatahubOutputFormat setTimeZone(String timeZone) {
		this.timeZone = timeZone;
		return this;
	}

	public DatahubOutputFormat setRowTypeInfo(RowTypeInfo rowTypeInfo) {
		this.rowTypeInfo = rowTypeInfo;
		return this;
	}

	public DatahubOutputFormat setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
		return this;
	}

	public DatahubOutputFormat setBatchSize(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	public DatahubOutputFormat setBatchWriteTimeout(long batchWriteTimeout) {
		this.batchWriteTimeout = batchWriteTimeout;
		return this;
	}

	public DatahubOutputFormat setPrintRecord(boolean printRecord) {
		isPrintRecord = printRecord;
		return this;
	}

	public DatahubOutputFormat setRetryTimeoutInMills(int retryTimeoutInMills) {
		this.retryTimeoutInMills = retryTimeoutInMills;
		return this;
	}

	public DatahubOutputFormat setMaxRetryTimes(int maxRetryTimes) {
		this.maxRetryTimes = maxRetryTimes;
		return this;
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		if (null == datahubClientProvider) {
			if (!StringUtils.isNullOrWhitespaceOnly(accessId) && !StringUtils.isNullOrWhitespaceOnly(accessKey)) {
				datahubClientProvider = new DatahubClientProvider(endPoint, accessId, accessKey);
			} else {
				datahubClientProvider = new DatahubClientProvider(endPoint, properties);
			}
		}

		client = datahubClientProvider.getClient();

		for (ShardEntry shardEntry : client.listShard(projectName, topicName).getShards()) {
			if (shardEntry.getState() == ShardState.ACTIVE) {
				shardIdList.add(shardEntry.getShardId());
			}
		}
		shardNum = shardIdList.size();

		outTps = MetricUtils.registerOutTps(getRuntimeContext());
		outBps = MetricUtils.registerOutBps(getRuntimeContext(), "datahub");
		latencyGauge = MetricUtils.registerOutLatency(getRuntimeContext());
		scheduleFlusher();

		recordResolver.open();
	}

	@Override
	public long getRetryTimeout() {
		return 30 * 1000;
	}

	@Override
	public void sync() throws IOException {
		if (putBuffer != null && putBuffer.size() > 0) {
			synchronized (putBuffer) {
				batchWrite(putBuffer);
				putBuffer.clear();
				lastWriteTime = System.currentTimeMillis();
			}
		}
	}

	private void writeDataHubPack(List<RecordEntry> message) {
		if (null == message || message.isEmpty()) {
			return;
		}

		int retryTime = 0;
		PutRecordsResult result;
		boolean flag = false;

		Random random = new Random();
		int writeIndex = Math.abs(random.nextInt()) % shardNum;

		String shardId = shardIdList.get(writeIndex);
		for (RecordEntry recordEntry : message) {
			recordEntry.setShardId(shardId);
		}

		long startTime = System.currentTimeMillis();
		String errorMessage = "";

		do {
			try {
				client = datahubClientProvider.getClient();
				result = client.putRecords(projectName, topicName, message);
				if (result.getFailedRecordCount() > 0) {
					message = result.getFailedRecords();
					for (PutErrorEntry entry : result.getPutErrorEntries()) {
						errorMessage = "write record retry failed " + retryTime
									+ " times, errorcode: [" + entry.getErrorcode()
									+ "], errorMsg: [" + entry.getMessage() + "]  retry total times:"
									+ String.valueOf(System.currentTimeMillis() - startTime) + " target shardId "
									+ String.valueOf(shardIdList.get(writeIndex));
						logger.error(errorMessage);
					}
					try {
						Thread.sleep(retryTime * retryTimeoutInMills);
					} catch (InterruptedException e1) {
						throw new RuntimeException(e1);
					}
				} else {
					long useTime = System.currentTimeMillis() - startTime;
					if (this.isPrintRecord) {
						logger.info("write shard index " + writeIndex + ", using time " + useTime);
					}
					flag = true;
				}
			} catch (Exception e) {
				// refresh sts account
				datahubClientProvider.getClient(true, true);
				flag = false;
				errorMessage = e.getMessage();
				logger.error("write data to datahub error" + retryTime + ", " + e.getMessage());
				try {
					Thread.sleep(retryTime * retryTimeoutInMills);
				} catch (InterruptedException e1) {
				}
			}
		} while ((!flag) && (retryTime++ < maxRetryTimes));

		if (!flag) {
			logger.error("Error after retry: " + retryTime + ", errorMsg: " + errorMessage);
			ErrorUtils.throwException(ConnectorErrors.INST.writeToDatahubError(String.valueOf(retryTime),
																			errorMessage));
		}
	}

	public void batchWrite(List<RecordEntry> buffer) {
		if (null != buffer && buffer.size() != 0) {
			long start = System.currentTimeMillis();
			long totalBytes = 0L;
			List<RecordEntry> recordPack = new ArrayList<RecordEntry>();
			for (RecordEntry recordEntry : buffer) {
				// default set each record as 1000 bytes.
				totalBytes += 1000;
				recordPack.add(recordEntry);
				if (recordPack.size() >= batchSize) {
					writeDataHubPack(recordPack);
					recordPack.clear();
				}
			}

			if (recordPack.size() != 0) {
				writeDataHubPack(recordPack);
				recordPack.clear();
			}

			// report metrics
			if (latencyGauge != null) {
				long end = System.currentTimeMillis();
				latencyGauge.report(end - start, buffer.size());
			}
			if (outTps != null) {
				outTps.markEvent(buffer.size());
			}
			if (outBps != null) {
				outBps.markEvent(totalBytes);
			}

			buffer.clear();
		}
	}

	@Override
	public void writeRecord(T record) throws IOException {
		if (flushError && null != flushException){
			throw new RuntimeException(flushException);
		}
		if (null != record) {
			synchronized (putBuffer) {
				putBuffer.add(recordResolver.getRecordEntry(record));
			}
		}

		if (putBuffer.size() >= bufferSize) {
			sync();
		}
	}

	@Override
	public void configure(Configuration configuration) {

	}

	@Override
	public void close() throws IOException {
		if (flusher != null) {
			flusher.cancel();
			flusher = null;
		}
		sync();
	}

	/**
	 * builder for DatahubOutputFormat.
	 */
	public static class Builder {
		private String endPoint;
		private String projectName;
		private String topicName;
		private String accessKey;
		private String accessId;

		private Configuration properties;
		private RowTypeInfo rowTypeInfo;
		private List<String> primaryKeys;

		private int bufferSize = 300;
		private int batchSize = 50;
		private long batchWriteTimeout = 20000; //20s如果没有攒批如果没有触发，则主动flush数据

		private boolean isPrintRecord = false;
		private int retryTimeoutInMills = 1000;
		private int maxRetryTimes = 3;
		private String timeZone;
		private DatahubRecordResolver recordResolver;

		public Builder setRecordResolver(
			DatahubRecordResolver recordResolver) {
			this.recordResolver = recordResolver;
			return this;
		}

		public Builder setTimeZone(String timeZone) {
			this.timeZone = timeZone;
			return this;
		}

		public Builder setEndPoint(String endPoint) {
			this.endPoint = endPoint;
			return this;
		}

		public Builder setProjectName(String projectName) {
			this.projectName = projectName;
			return this;
		}

		public Builder setTopicName(String topicName) {
			this.topicName = topicName;
			return this;
		}

		public String getTopicName() {
			return  this.topicName;
		}

		public Builder setAccessKey(String accessKey) {
			this.accessKey = accessKey;
			return this;
		}

		public Builder setAccessId(String accessId) {
			this.accessId = accessId;
			return this;
		}

		public Builder setPrimaryKeys(List<String> primaryKeys) {
			this.primaryKeys = primaryKeys;
			return this;
		}

		public Builder setProperties(Configuration properties) {
			this.properties = properties;
			return this;
		}

		public Builder setBufferSize(int bufferSize) {
			this.bufferSize = bufferSize;
			return this;
		}

		public Builder setBatchSize(int batchSize) {
			this.batchSize = batchSize;
			return this;
		}

		public Builder setBatchWriteTimeout(long batchWriteTimeout) {
			this.batchWriteTimeout = batchWriteTimeout;
			return this;
		}

		public Builder setPrintRecord(boolean printRecord) {
			isPrintRecord = printRecord;
			return this;
		}

		public Builder setRetryTimeoutInMills(int retryTimeoutInMills) {
			this.retryTimeoutInMills = retryTimeoutInMills;
			return this;
		}

		public Builder setMaxRetryTimes(int maxRetryTimes) {
			this.maxRetryTimes = maxRetryTimes;
			return this;
		}

		public Builder setRowTypeInfo(RowTypeInfo rowTypeInfo) {
			this.rowTypeInfo = rowTypeInfo;
			return this;
		}

		public DatahubOutputFormat build() {
			DatahubOutputFormat outputFormat = null;
			if (StringUtils.isNullOrWhitespaceOnly(accessId) || StringUtils.isNullOrWhitespaceOnly(accessKey)) {
				outputFormat = new DatahubOutputFormat(
						endPoint,
						projectName,
						topicName,
						properties,
						rowTypeInfo);
			} else {
				outputFormat = new DatahubOutputFormat(
						endPoint,
						projectName,
						topicName,
						accessId,
						accessKey,
						rowTypeInfo);
			}
			outputFormat.setBatchSize(batchSize)
						.setBatchWriteTimeout(batchWriteTimeout)
						.setMaxRetryTimes(maxRetryTimes)
						.setBatchWriteTimeout(batchWriteTimeout)
						.setPrintRecord(isPrintRecord)
						.setRowTypeInfo(rowTypeInfo)
						.setTimeZone(timeZone)
						.setBufferSize(bufferSize)
						.setRecordResolver(recordResolver);
			return outputFormat;
		}

	}
}
