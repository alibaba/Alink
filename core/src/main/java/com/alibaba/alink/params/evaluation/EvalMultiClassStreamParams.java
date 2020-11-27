package com.alibaba.alink.params.evaluation;

import com.alibaba.alink.params.shared.HasTimeIntervalDv3;

/**
 * Params for multi classification evaluation.
 */
public interface EvalMultiClassStreamParams<T> extends
	EvalMultiClassParams <T>,
	HasTimeIntervalDv3 <T> {
}
