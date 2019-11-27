package com.alibaba.alink.params.evaluation;

import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasPredictionDetailCol;
import com.alibaba.alink.params.shared.linear.HasPositiveLabelValueString;

/**
 * Params for binary classification evaluation.
 */
public interface BinaryEvaluationParams<T> extends
	HasLabelCol<T>,
	HasPredictionDetailCol<T>,
	HasPositiveLabelValueString <T> {
}
