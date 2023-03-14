package com.alibaba.alink.operator.common.linear;

import com.alibaba.alink.operator.common.finance.stepwiseSelector.RegressionSelectorStep;
import com.alibaba.alink.operator.common.finance.stepwiseSelector.SelectorStep;

public class LinearRegressionSummary extends ModelSummary {

	public double ra2;
	public double r2;
	public double mallowCp;
	public double fValue;
	public double pValue;
	public double sse;
	public double[] stdEsts;
	public double[] stdErrs;
	public double[] tValues;
	public double[] tPVaues;
	public double[] lowerConfidence;
	public double[] uperConfidence;

	@Override
	public SelectorStep toSelectStep(int inId) {
		RegressionSelectorStep step = new RegressionSelectorStep();
		step.enterCol = String.valueOf(inId);
		step.fValue = this.fValue;
		step.mallowCp = this.mallowCp;
		step.r2 = this.r2;
		step.ra2 = this.ra2;
		step.pValue = this.pValue;
		step.numberIn = this.beta.size() - 1;

		return step;
	}
}
