package com.alibaba.alink.params.regression;

import com.alibaba.alink.params.shared.linear.HasLambda;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;

/**
 * parameters of lasso regression train process.
 *
 */
public interface LassoRegTrainParams<T> extends
	LinearTrainParams <T>,
	HasLambda <T> {}
