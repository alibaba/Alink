package com.alibaba.alink.operator.local.classification;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.linear.LinearClassifierModelInfo;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.lazy.ExtractModelInfoLocalOp;

import java.util.List;

/**
 * Linear classifier model info batch op.
 */
public class LogisticRegressionModelInfoLocalOp
	extends ExtractModelInfoLocalOp <LinearClassifierModelInfo, LogisticRegressionModelInfoLocalOp> {

	private static final long serialVersionUID = -3598960496955727614L;

	public LogisticRegressionModelInfoLocalOp() {
		this(null);
	}

	public LogisticRegressionModelInfoLocalOp(Params params) {
		super(params);
	}

	@Override
	protected LinearClassifierModelInfo createModelInfo(List <Row> rows) {
		return new LinearClassifierModelInfo(rows);
	}

	@Override
	protected LocalOperator <?> processModel() {
		return this;
	}
}
