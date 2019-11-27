package com.alibaba.alink.params.regression;

import com.alibaba.alink.params.shared.linear.HasLambda;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;

/**
 * parameters of ridge regression train process.
 *
 */
public interface RidgeRegTrainParams<T> extends
	LinearTrainParams <T>,
	HasLambda <T> {}
