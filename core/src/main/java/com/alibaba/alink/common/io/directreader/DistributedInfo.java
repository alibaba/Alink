package com.alibaba.alink.common.io.directreader;

import java.io.Serializable;

/**
 * An util class to compute data partition.
 */
public interface DistributedInfo extends Serializable {

	/**
	 * Get the start position.
	 *
	 * @param taskId       id of current task.
	 * @param parallelism  total parallelism of all tasks.
	 * @param globalRowCnt total row count.
	 * @return start position of this task.
	 */
	long startPos(long taskId, long parallelism, long globalRowCnt);

	/**
	 * Get the row count for this task.
	 *
	 * @param taskId       id of current task.
	 * @param parallelism  total parallelism of all tasks.
	 * @param globalRowCnt total row count.
	 * @return start position of this task.
	 */
	long localRowCnt(long taskId, long parallelism, long globalRowCnt);

	long where(long pos, long parallelism, long globalRowCnt);
}
