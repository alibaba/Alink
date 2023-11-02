package com.alibaba.alink.params.evaluation;

import com.alibaba.alink.params.shared.HasTimeIntervalDefaultAs3;

/**
 * Params for regression evaluation.
 */
public interface EvalRegressionStreamParams<T> extends
	EvalRegressionParams<T>,
	HasTimeIntervalDefaultAs3 <T> {
}
