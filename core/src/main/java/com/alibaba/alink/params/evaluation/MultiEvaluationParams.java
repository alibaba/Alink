package com.alibaba.alink.params.evaluation;

import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasPredictionDetailCol;

/**
 * Params for multi classification evaluation.
 */
public interface MultiEvaluationParams<T> extends
	HasLabelCol <T>,
	HasPredictionCol <T>,
	HasPredictionDetailCol <T> {
}
