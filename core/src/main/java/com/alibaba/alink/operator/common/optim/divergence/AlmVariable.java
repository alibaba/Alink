package com.alibaba.alink.operator.common.optim.divergence;

import com.alibaba.alink.operator.common.optim.activeSet.ConstraintVariable;

public class AlmVariable extends ConstraintVariable {
	public final static String gradVariableForReduce = "gradVariableForReduce";
	public final static String lossVariableForReduce = "lossVariableForReduce";
	public final static String oldK = "oldK";
	public final static String k = "k";
	public final static String maxKIterStep = "maxKIterStep";
	public final static String numInvalid = "numInvalid";
	public final static String c = "c";
	public final static String iter = "iter";
	public final static String minIterTime = "minIterTime";
	public final static String lastValidIter = "lastValidIter";
	public final static String alpha = "alpha";
	public final static String lambda = "lambda";
	public final static String mu = "mu";
	public final static String oldWeight = "oldWeight";
	public final static String oldDir = "oldDir";
	public final static String retryNum = "retryNum";
	public final static String convergenceTolerance = "convergenceTolerance";
	public final static String weight = "weight";
	public final static String kConvergence = "kConvergence";
	public final static String equalityResidual = "equalityResidual";
	public final static String inequalityResidual = "inequalityResidual";
	public final static String lambdaSize = "lambdaSize";
	public final static String isConvergence = "isConvergence";
}
