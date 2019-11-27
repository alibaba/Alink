package com.alibaba.alink.common.comqueue;

/**
 * An BaseComQueue item for computation.
 */
public abstract class ComputeFunction implements ComQueueItem {

	/**
	 * Perform the computation work.
	 *
	 * @param context to get input object and update output object.
	 */
	public abstract void calc(ComContext context);
}
