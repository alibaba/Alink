package com.alibaba.alink.operator.local.regression;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.linear.LinearRegressorModelInfo;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.lazy.ExtractModelInfoLocalOp;

import java.util.List;

public class RidgeRegModelInfoLocalOp
	extends ExtractModelInfoLocalOp <LinearRegressorModelInfo, RidgeRegModelInfoLocalOp> {

	public RidgeRegModelInfoLocalOp() {
		this(null);
	}

	public RidgeRegModelInfoLocalOp(Params params) {
		super(params);
	}

	@Override
	protected LinearRegressorModelInfo createModelInfo(List <Row> rows) {
		return new LinearRegressorModelInfo(rows);
	}

}
