package com.alibaba.alink.params.shared.optim;

import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;
import com.alibaba.alink.params.shared.linear.HasEpsilonDv0000001;

/**
 * parameters of sgd.
 *
 */
public interface SgdParams<T> extends
	HasMaxIterDefaultAs100<T>,
	HasMiniBatchFractionDv01 <T>,
	HasEpsilonDv0000001 <T>,
	HasLearningRateDv01 <T> {
}
