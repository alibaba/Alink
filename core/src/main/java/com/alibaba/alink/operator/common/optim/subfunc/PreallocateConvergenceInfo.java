package com.alibaba.alink.operator.common.optim.subfunc;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;

import java.util.Arrays;

/**
 * Preallocate memory for loss value of every iteration.
 */
public class PreallocateConvergenceInfo extends ComputeFunction {
	private static final long serialVersionUID = 3801536180940080L;
	private String name;
	private int maxIter;

	/**
	 * @param name    name for this memory.
	 * @param lossNum num of loss values.
	 */
	public PreallocateConvergenceInfo(String name, int lossNum) {
		this.name = name;
		this.maxIter = lossNum;
	}

	@Override
	public void calc(ComContext context) {
		if (context.getStepNo() == 1) {
			double[] lossCurve = new double[maxIter * 3];
			Arrays.fill(lossCurve, Double.POSITIVE_INFINITY);
			context.putObj(name, lossCurve);
		}
	}
}
