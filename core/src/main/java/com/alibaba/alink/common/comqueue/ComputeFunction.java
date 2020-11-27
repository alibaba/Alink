package com.alibaba.alink.common.comqueue;

/**
 * An BaseComQueue item for computation.
 */
public abstract class ComputeFunction implements ComQueueItem {

	private static final long serialVersionUID = -6463146823942978868L;

	/**
	 * Perform the computation work.
	 *
	 * @param context to get input object and update output object.
	 */
	public abstract void calc(ComContext context);
}
