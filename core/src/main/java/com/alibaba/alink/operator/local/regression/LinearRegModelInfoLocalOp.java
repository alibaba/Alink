package com.alibaba.alink.operator.local.regression;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.linear.LinearRegressorModelInfo;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.lazy.ExtractModelInfoLocalOp;

import java.util.List;

public class LinearRegModelInfoLocalOp
	extends ExtractModelInfoLocalOp <LinearRegressorModelInfo, LinearRegModelInfoLocalOp> {

	public LinearRegModelInfoLocalOp() {
		this(null);
	}

	public LinearRegModelInfoLocalOp(Params params) {
		super(params);
	}

	@Override
	protected LinearRegressorModelInfo createModelInfo(List <Row> rows) {
		return new LinearRegressorModelInfo(rows);
	}

}
