package com.alibaba.alink.common.io.directreader;


/**
 * An range based implementation of {@link DistributedInfo}.
 */
public class DefaultDistributedInfo implements DistributedInfo {

	@Override
	public long startPos(long taskId, long parallelism, long globalRowCnt) {
		long div = globalRowCnt / parallelism;
		long mod = globalRowCnt % parallelism;

		if (mod == 0) {
			return div * taskId;
		} else if (taskId >= mod) {
			return div * taskId + mod;
		} else {
			return div * taskId + taskId;
		}
	}

	@Override
	public long localRowCnt(long taskId, long parallelism, long globalRowCnt) {
		long div = globalRowCnt / parallelism;
		long mod = globalRowCnt % parallelism;

		if (mod == 0) {
			return div;
		} else if (taskId >= mod) {
			return div;
		} else {
			return div + 1;
		}
	}
}
