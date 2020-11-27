package com.alibaba.alink.operator.common.fm;

import org.apache.flink.types.Row;

import java.util.List;

/**
 * Fm classifier model train info.
 */
public final class FmClassifierModelTrainInfo extends FmRegressorModelTrainInfo {

	private static final long serialVersionUID = 1729981129020285530L;

	public FmClassifierModelTrainInfo(List <Row> rows) {
		super(rows);
	}

	@Override
	protected void setKeys() {
		keys = new String[] {" auc: ", " accuracy: "};
	}
}
