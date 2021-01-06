package com.alibaba.alink.common.io.catalog.odps;

import org.apache.flink.api.java.tuple.Tuple2;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class OdpsInputSplitReader implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(OdpsInputSplitReader.class);
	private final OdpsPartitionSegmentDownloadDesc[] partSegments;
	private final List <Column> odpsColumns;
	private final TableTunnel tableTunnel;
	private final String project;
	private final String table;
	private TunnelRecordReader recordReader;
	private int currentPartSegmentIdx;
	private OdpsPartitionSegmentDownloadDesc currentPartitionDownloadDesc;
	private PartitionSpec currentPartitionSpec;
	private boolean reachEnd;
	private int position;
	private final long retryTimes;
	private final long sleepTimeMs;
	private final int[] RETRY_BACKOFF;

	public OdpsInputSplitReader(OdpsInputSplit split, String[] selectedColumns, OdpsTableSchema tableSchema,
								TableTunnel tableTunnel, String project, String table) {
		this.retryTimes = 3L;
		this.sleepTimeMs = 1000L;
		this.RETRY_BACKOFF = new int[] {1, 2, 3, 5, 10, 20, 40, 100, 100, 100, 100, 200, 200};
		this.partSegments = split.getPartitions();
		if (selectedColumns != null) {
			List <Column> tmpColums = new ArrayList<>();

			for (String columnName : selectedColumns) {
				if (columnName != null && !tableSchema.isPartitionColumn(columnName)) {
					OdpsColumn column = tableSchema.getColumn(columnName);
					tmpColums.add(new Column(column.getName(), column.getType()));
				}
			}

			this.odpsColumns = tmpColums;
		} else {
			this.odpsColumns = null;
		}

		this.tableTunnel = tableTunnel;
		this.project = project;
		this.table = table;
		this.reachEnd = false;
		this.currentPartSegmentIdx = -1;
		this.position = 0;
		this.openNextPartSegment();
	}

	public boolean hasNext() {
		return !this.reachEnd;
	}

	public Tuple2 <Record, PartitionSpec> next() throws IOException, InterruptedException {
		Record record;
		int retriedTime = 0;

		while (true) {
			try {
				record = this.recordReader.read();
				++this.position;
				break;
			} catch (IOException ex) {
				if ((long) retriedTime >= this.retryTimes) {
					throw new IOException("Failed to read next message within " + retriedTime + "retries", ex);
				}

				Thread.sleep(this.sleepTimeMs);
				LOG.warn("Occur Odps record reader issues, reconnect after " + this.sleepTimeMs + "ms", ex);
				this.recordReader = this.createRecordReader();
				++retriedTime;
			}
		}

		if (record != null) {
			return Tuple2.of(record, this.currentPartitionSpec);
		} else {
			IOUtils.closeQuietly(this.recordReader);
			return this.openNextPartSegment() ? this.next() : null;
		}
	}

	public void close() {
		if (this.recordReader != null) {
			IOUtils.closeQuietly(this.recordReader);
			this.recordReader = null;
		}

	}

	private TunnelRecordReader createRecordReader() {
		int retries = 0;

		while (true) {
			try {
				DownloadSession downloadSession;
				if (this.currentPartitionSpec == null) {
					downloadSession = this.tableTunnel.getDownloadSession(this.project, this.table,
						this.currentPartitionDownloadDesc.getDownloadSessionID());
				} else {
					downloadSession = this.tableTunnel.getDownloadSession(this.project, this.table,
						this.currentPartitionSpec, this.currentPartitionDownloadDesc.getDownloadSessionID());
				}

				return downloadSession.openRecordReader(
					this.currentPartitionDownloadDesc.getStart() + (long) this.position,
					this.currentPartitionDownloadDesc.getCount() - (long) this.position, true, this.odpsColumns);
			} catch (IOException | TunnelException ex) {
				if (ex instanceof TunnelException && "StatusConflict".equalsIgnoreCase(
					((TunnelException) ex).getErrorCode())) {
					this.handleSessionTimeOut();
				}

				if (this.currentPartitionSpec == null) {
					LOG.warn("fail to open record reader of odps table [{}]", this.table, ex);
				} else {
					LOG.warn("fail to open record reader of odps table [{}] on partition [{}]",
						this.table, this.currentPartitionSpec.toString(), ex);
				}

				if (retries++ >= 21) {
					LOG.warn("reach max retries limit to open record reader of odps table [{}] on partition [{}]",
						this.table, this.currentPartitionSpec == null ? "null" : this.currentPartitionSpec.toString());
					throw new RuntimeException("fail to open odps record reader of table for {} times21 times", ex);
				}

				try {
					Thread.sleep(this.getPauseTime(retries));
				} catch (InterruptedException var4) {
					LOG.warn("fail to open record reader of table", var4);
					throw new RuntimeException("fail to open record reader of table", var4);
				}
			}
		}
	}

	private boolean openNextPartSegment() {
		if (this.currentPartSegmentIdx == this.partSegments.length - 1) {
			this.reachEnd = true;
			return false;
		} else {
			this.currentPartitionDownloadDesc = this.partSegments[++this.currentPartSegmentIdx];
			this.currentPartitionSpec = this.currentPartitionDownloadDesc.getPartitionSpec();
			if (this.currentPartitionDownloadDesc.getCount() != 0L) {
				this.position = 0;
				this.recordReader = this.createRecordReader();
				return true;
			} else {
				return this.openNextPartSegment();
			}
		}
	}

	private long getPauseTime(int tries) {
		int nTries = tries >= this.RETRY_BACKOFF.length ? this.RETRY_BACKOFF.length - 1 : tries;
		if (nTries < 0) {
			nTries = 0;
		}

		long normalPauseTime = 50L * this.RETRY_BACKOFF[nTries];
		long jitter = (long) ((float) normalPauseTime * ThreadLocalRandom.current().nextFloat() * 0.01F);
		return normalPauseTime + jitter;
	}

	private void handleSessionTimeOut() {
		DownloadSession downloadSession = null;

		try {
			if (this.currentPartitionSpec == null) {
				downloadSession = this.tableTunnel.createDownloadSession(this.project, this.table);
			} else {
				downloadSession = this.tableTunnel.createDownloadSession(this.project, this.table,
					this.currentPartitionSpec);
			}
		} catch (TunnelException ex) {
			if (this.currentPartitionSpec == null) {
				LOG.warn("fail to create download session to table [{}] in project [{}]",
					this.table, this.project, ex);
			} else {
				LOG.warn("fail to create download session to partition [{}] on table [{}] in project [{}]",
					this.currentPartitionSpec.toString(), this.table, this.project, ex);
			}

			return;
		}

		String expireDownloadSessionID = this.currentPartitionDownloadDesc.getDownloadSessionID();
		this.currentPartitionDownloadDesc.setDownloadSessionID(downloadSession.getId());

		for (OdpsPartitionSegmentDownloadDesc partSegment : this.partSegments) {
			if (partSegment.getDownloadSessionID().equals(expireDownloadSessionID)) {
				partSegment.setDownloadSessionID(downloadSession.getId());
			}
		}

	}
}