package com.alibaba.alink.common.io.catalog.odps;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.util.Preconditions;

public class OdpsInputSplit implements InputSplit {
	private static final long serialVersionUID = 1L;
	private final int splitId;
	private final OdpsPartitionSegmentDownloadDesc[] partitions;

	public OdpsInputSplit(int splitId, OdpsPartitionSegmentDownloadDesc[] partitions) {
		Preconditions.checkArgument(splitId >= 0, "splitId is less than zero!");
		this.splitId = splitId;
		this.partitions = Preconditions.checkNotNull(partitions, "partitions is null!");
	}

	public OdpsPartitionSegmentDownloadDesc[] getPartitions() {
		return this.partitions;
	}

	public int getSplitNumber() {
		return this.splitId;
	}
}
