/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.alibaba.alink.operator.common.regression;

import com.alibaba.alink.common.utils.JsonConverter;

/**
 * @author yangxu
 */
public class LinearRegressionStepwiseModel implements RegressionModelInterface {

	public LinearRegressionModel lrr;
	public String stepInfo;

	LinearRegressionStepwiseModel(LinearRegressionModel lrr, String stepInfo) {
		this.lrr = lrr;
		this.stepInfo = stepInfo;
	}

	@Override
	public String toString() {
		return lrr.toString() + "\nLinear Regression Step Info:\n" + stepInfo;
	}

	public String __repr__() {
		return toString();
	}

	public String toJson() {
		return JsonConverter.gson.toJson(this);

	}
}
