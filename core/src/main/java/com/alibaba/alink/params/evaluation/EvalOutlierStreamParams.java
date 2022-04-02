package com.alibaba.alink.params.evaluation;

import com.alibaba.alink.params.shared.HasTimeIntervalDefaultAs3;

/**
 * Params for outlier evaluation.
 */
public interface EvalOutlierStreamParams<T>
	extends EvalOutlierParams <T>, HasTimeIntervalDefaultAs3 <T> {
}
