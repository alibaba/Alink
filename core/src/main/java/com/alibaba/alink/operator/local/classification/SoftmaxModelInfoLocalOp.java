package com.alibaba.alink.operator.local.classification;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.linear.SoftmaxModelInfo;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.lazy.ExtractModelInfoLocalOp;

import java.util.List;

public class SoftmaxModelInfoLocalOp
	extends ExtractModelInfoLocalOp <SoftmaxModelInfo, SoftmaxModelInfoLocalOp> {

	public SoftmaxModelInfoLocalOp() {
		this(null);
	}

	public SoftmaxModelInfoLocalOp(Params params) {
		super(params);
	}

	@Override
	protected SoftmaxModelInfo createModelInfo(List <Row> rows) {
		return new SoftmaxModelInfo(rows);
	}

	@Override
	protected LocalOperator <?> processModel() {
		return this;
	}
}
