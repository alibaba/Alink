package com.alibaba.alink.params.classification;

import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.HasL2;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;

/**
 * parameters of binary class training process.
 *
 */
public interface LinearBinaryClassTrainParams<T> extends
	LinearTrainParams <T>,
	HasL1 <T>,
	HasL2 <T> {}
