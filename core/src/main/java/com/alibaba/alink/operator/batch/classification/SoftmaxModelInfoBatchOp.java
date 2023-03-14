package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.utils.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.linear.SoftmaxModelInfo;

import java.util.List;

public class SoftmaxModelInfoBatchOp
	extends ExtractModelInfoBatchOp <SoftmaxModelInfo, SoftmaxModelInfoBatchOp> {

	private static final long serialVersionUID = -3271395248030822295L;

	public SoftmaxModelInfoBatchOp() {
		this(null);
	}

	public SoftmaxModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected SoftmaxModelInfo createModelInfo(List <Row> rows) {
		return new SoftmaxModelInfo(rows);
	}

}
