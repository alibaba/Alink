package com.alibaba.alink.params.evaluation;

import com.alibaba.alink.params.shared.HasTimeIntervalDefaultAs3;

/**
 * Params for multi classification evaluation.
 */
public interface EvalMultiClassStreamParams<T> extends
	EvalMultiClassParams <T>,
	HasTimeIntervalDefaultAs3 <T> {
}
