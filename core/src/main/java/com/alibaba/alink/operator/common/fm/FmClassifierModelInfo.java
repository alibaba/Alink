package com.alibaba.alink.operator.common.fm;

import org.apache.flink.types.Row;

import java.util.List;

/**
 * Model info of FmClassifier.
 */
public class FmClassifierModelInfo extends FmRegressorModelInfo {

	private static final long serialVersionUID = 1393682477554387382L;

	public Object[] getLabelValues() {
		return this.labelValues;
	}

	public FmClassifierModelInfo(List <Row> rows) {
		super(rows);
	}

	@Override
	protected void processLabelValues(FmModelData modelData) {
		this.labelValues = modelData.labelValues;
	}
}
