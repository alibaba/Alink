package com.alibaba.alink.operator.common.optim.activeSet;

import com.alibaba.alink.operator.common.optim.subfunc.OptimVariable;

public class ConstraintVariable extends OptimVariable {
	public final static String constraints = "constraints";
	public final static String weightDim = "dim";
	public final static String lossAllReduce = "lossAllReduce";
	public final static String lastLoss = "lastLoss";
	public final static String loss = "loss";
	public final static String convergence = "convergence";
	public final static String weight = "weight";
	public final static String linearSearchTimes = "linearSearchTimes";
	public final static String minL2Weight = "minL2Weight";
	public final static String newtonRetryTime = "retryTime";
}