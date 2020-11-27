package com.alibaba.alink.common.io.directreader;

/**
 * An range based implementation of {@link DistributedInfo}.
 */
public final class DefaultDistributedInfo implements DistributedInfo {

	private static final long serialVersionUID = -6343669035998389436L;

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

	@Override
	public long where(long pos, long parallelism, long globalRowCnt) {
		long div = globalRowCnt / parallelism;
		long mod = globalRowCnt % parallelism;
		if (div == 0) {
			return pos;
		}
		if ((pos - mod) / div >= mod) {
			return (pos - mod) / div;
		}
		return pos / (div + 1);
	}
}
