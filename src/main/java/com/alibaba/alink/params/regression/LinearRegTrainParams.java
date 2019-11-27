package com.alibaba.alink.params.regression;

import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.HasL2;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;

/**
 * parameters of linear regression train process.
 *
 */
public interface LinearRegTrainParams<T> extends
	LinearTrainParams <T>,
	HasL1 <T>,
	HasL2 <T> {}
