package com.alibaba.alink.operator.local.regression;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.linear.LinearRegressorModelInfo;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.lazy.ExtractModelInfoLocalOp;

import java.util.List;

public class LassoRegModelInfoLocalOp
	extends ExtractModelInfoLocalOp <LinearRegressorModelInfo, LassoRegModelInfoLocalOp> {

	public LassoRegModelInfoLocalOp() {
		this(null);
	}

	public LassoRegModelInfoLocalOp(Params params) {
		super(params);
	}

	@Override
	protected LinearRegressorModelInfo createModelInfo(List <Row> rows) {
		return new LinearRegressorModelInfo(rows);
	}

	@Override
	protected LocalOperator <?> processModel() {
		return this;
	}
}
