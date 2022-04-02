package com.alibaba.alink.params.evaluation;

import com.alibaba.alink.params.shared.colname.HasLabelCol;

/**
 * Params for outlier evaluation.
 */
public interface EvalOutlierParams<T>
	extends HasLabelCol <T>, HasOutlierValueStrings <T>, HasPredictionDetailColRequired <T> {
}
