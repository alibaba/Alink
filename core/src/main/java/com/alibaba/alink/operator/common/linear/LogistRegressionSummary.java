package com.alibaba.alink.operator.common.linear;

import com.alibaba.alink.operator.common.finance.stepwiseSelector.ClassificationSelectorStep;
import com.alibaba.alink.operator.common.finance.stepwiseSelector.SelectorStep;

public class LogistRegressionSummary extends ModelSummary {

	public double scoreChiSquareValue;
	public double scorePValue;
	public double aic;
	public double sc;
	public double[] stdEsts;
	public double[] stdErrs;
	public double[] waldChiSquareValue;
	public double[] waldPValues;
	public double[] lowerConfidence;
	public double[] uperConfidence;

	@Override
	public SelectorStep toSelectStep(int inId) {
		ClassificationSelectorStep step = new ClassificationSelectorStep();
		step.enterCol = String.valueOf(inId);
		step.scoreValue = this.scoreChiSquareValue;
		step.pValue = this.scorePValue;
		step.numberIn = this.beta.size() - 1;

		return step;
	}
}
