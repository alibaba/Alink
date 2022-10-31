package com.alibaba.alink.operator.local.dataproc;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.dataproc.MaxAbsScalarModelInfo;
import com.alibaba.alink.operator.local.lazy.ExtractModelInfoLocalOp;

import java.util.List;

public class MaxAbsScalerModelInfoLocalOp
	extends ExtractModelInfoLocalOp <MaxAbsScalarModelInfo, MaxAbsScalerModelInfoLocalOp> {

	public MaxAbsScalerModelInfoLocalOp() {
		this(null);
	}

	public MaxAbsScalerModelInfoLocalOp(Params params) {
		super(params);
	}

	@Override
	protected MaxAbsScalarModelInfo createModelInfo(List <Row> rows) {
		return new MaxAbsScalarModelInfo(rows);
	}
}
