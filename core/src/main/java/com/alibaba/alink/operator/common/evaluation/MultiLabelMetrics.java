package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * Metrics for multiLabel evaluation.
 */
public class MultiLabelMetrics extends BaseSimpleMultiLabelMetrics <MultiLabelMetrics> {
	private static final long serialVersionUID = -7162025207527000746L;

	public MultiLabelMetrics(Row row) {
		super(row);
	}

	public MultiLabelMetrics(Params params) {
		super(params);
	}
}
