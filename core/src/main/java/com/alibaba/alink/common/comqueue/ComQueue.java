package com.alibaba.alink.common.comqueue;

/**
 * An BaseComQueue with only single iteration.
 */
public class ComQueue extends BaseComQueue <ComQueue> {

	public ComQueue() {
		super();
		setMaxIter(1);
	}

}
