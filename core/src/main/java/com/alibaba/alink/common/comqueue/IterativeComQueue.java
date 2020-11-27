package com.alibaba.alink.common.comqueue;

/**
 * The default implementation of BaseComQueue.
 */
public class IterativeComQueue extends BaseComQueue <IterativeComQueue> {

	private static final long serialVersionUID = 9183390606304249142L;

	public IterativeComQueue() {
		super();
	}

	@Override
	public IterativeComQueue setCompareCriterionOfNode0(CompareCriterionFunction compareCriterion) {
		return super.setCompareCriterionOfNode0(compareCriterion);
	}

	@Override
	public IterativeComQueue setMaxIter(int maxIter) {
		return super.setMaxIter(maxIter);
	}

}
