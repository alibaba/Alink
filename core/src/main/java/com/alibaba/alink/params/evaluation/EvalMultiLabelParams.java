package com.alibaba.alink.params.evaluation;

import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;

/**
 * Params for MultiLabel classification.
 */
public interface EvalMultiLabelParams<T>
	extends HasLabelCol <T>,
	HasPredictionRankingInfo <T>,
	HasLabelRankingInfo <T>,
	HasPredictionCol <T> {
}
