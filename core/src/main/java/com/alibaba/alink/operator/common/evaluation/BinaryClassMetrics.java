package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * Binary classification evaluation metrics.
 */
public class BinaryClassMetrics extends BaseBinaryClassMetrics <BinaryClassMetrics> {
	private static final long serialVersionUID = -2742232353232295684L;

	public BinaryClassMetrics(Row row) {
		super(row);
	}

	public BinaryClassMetrics(Params params) {
		super(params);
	}
}
