package com.alibaba.alink.operator.common.linear;

import org.apache.flink.types.Row;

import java.util.List;

/**
 * Linear classifier (lr, svm) model info.
 */
public class LinearClassifierModelInfo extends LinearRegressorModelInfo {

	private static final long serialVersionUID = 7233099674515498636L;

	public Object[] getLabelValues() {
		return this.labelValues;
	}

	public LinearClassifierModelInfo(List <Row> rows) {
		super(rows);
	}

	@Override
	protected void processLabelValues(LinearModelData modelData) {
		this.labelValues = modelData.labelValues;
	}
}
