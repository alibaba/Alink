package com.alibaba.alink.common.io.catalog.odps;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import com.aliyun.odps.PartitionSpec;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

public class OdpsPartitionSegmentDownloadDesc implements Serializable {
	private static final long serialVersionUID = -3786613257670817735L;
	private final String partition;
	private transient PartitionSpec partitionSpec;
	private final long start;
	private final long count;
	private String downloadSessionID;

	public OdpsPartitionSegmentDownloadDesc(@Nullable String partition, long start, long count, String downloadSessionID) {
		Preconditions.checkArgument(StringUtils.isNotBlank(downloadSessionID), "downloadSessionID is whitespace or null!");
		Preconditions.checkArgument(start >= 0L, "start is less than zero!");
		Preconditions.checkArgument(count > 0L, "count is not bigger than zero!");
		this.partition = partition;
		this.buildPartitionSpec();
		this.start = start;
		this.count = count;
		this.downloadSessionID = downloadSessionID;
	}

	public String getPartition() {
		return this.partition;
	}

	public long getStart() {
		return this.start;
	}

	public long getCount() {
		return this.count;
	}

	public String getDownloadSessionID() {
		return this.downloadSessionID;
	}

	public void setDownloadSessionID(String downloadSessionID) {
		this.downloadSessionID = downloadSessionID;
	}

	public PartitionSpec getPartitionSpec() {
		return this.partitionSpec;
	}

	private void readObject(ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
		inputStream.defaultReadObject();
		this.buildPartitionSpec();
	}

	private void buildPartitionSpec() {
		this.partitionSpec = this.partition == null ? null : new PartitionSpec(this.partition);
	}

	public String toString() {
		return "ODPSPartitionSegmentDownloadDesc{partition='" + this.partition + '\'' + ", partitionSpec=" + this.partitionSpec + ", start=" + this.start + ", count=" + this.count + ", downloadSessionID='" + this.downloadSessionID + '\'' + '}';
	}
}
