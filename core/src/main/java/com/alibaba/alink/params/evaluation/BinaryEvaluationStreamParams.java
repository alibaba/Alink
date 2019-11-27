package com.alibaba.alink.params.evaluation;

import com.alibaba.alink.params.shared.linear.HasPositiveLabelValueString;

/**
 * Params for binary classification evaluation.
 */
public interface BinaryEvaluationStreamParams<T> extends
	MultiEvaluationStreamParams <T>,
	HasPositiveLabelValueString <T> {
}
