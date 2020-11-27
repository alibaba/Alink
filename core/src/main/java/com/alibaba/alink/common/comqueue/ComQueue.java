package com.alibaba.alink.common.comqueue;

/**
 * An BaseComQueue with only single iteration.
 */
public class ComQueue extends BaseComQueue <ComQueue> {

	private static final long serialVersionUID = -1817153124048872526L;

	public ComQueue() {
		super();
		setMaxIter(1);
	}

}
