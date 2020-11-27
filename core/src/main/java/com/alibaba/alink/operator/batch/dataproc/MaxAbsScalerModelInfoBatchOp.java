package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.dataproc.MaxAbsScalarModelInfo;

import java.util.List;

public class MaxAbsScalerModelInfoBatchOp
	extends ExtractModelInfoBatchOp <MaxAbsScalarModelInfo, MaxAbsScalerModelInfoBatchOp> {

	private static final long serialVersionUID = 8092004475546948679L;

	public MaxAbsScalerModelInfoBatchOp() {
		this(null);
	}

	public MaxAbsScalerModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected MaxAbsScalarModelInfo createModelInfo(List <Row> rows) {
		return new MaxAbsScalarModelInfo(rows);
	}
}
