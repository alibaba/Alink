package com.alibaba.alink.params.evaluation;

import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;

/**
 * Params for regression evaluation.
 */
public interface EvalRegressionParams<T> extends
	HasLabelCol <T>,
	HasPredictionCol <T> {
}
