package com.alibaba.alink.params.shared.optim;

import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;
import com.alibaba.alink.params.shared.linear.HasEpsilonDefaultAs0000001;

/**
 * parameters of sgd.
 */
public interface SgdParams<T> extends
	HasMaxIterDefaultAs100 <T>,
	HasMiniBatchFractionDefaultAs01 <T>,
	HasEpsilonDefaultAs0000001 <T>,
	HasLearningRateDefaultAs01 <T> {
}
