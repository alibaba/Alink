package com.alibaba.alink.params.classification;

import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.HasL2;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;

/**
 * parameters of softmax training process.
 *
 */
public interface SoftmaxTrainParams<T> extends
	LinearTrainParams <T>,
	HasL1 <T>,
	HasL2 <T> {}
