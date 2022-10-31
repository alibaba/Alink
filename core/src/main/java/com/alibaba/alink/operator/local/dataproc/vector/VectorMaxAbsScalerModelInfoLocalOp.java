package com.alibaba.alink.operator.local.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.dataproc.vector.VectorMaxAbsScalarModelInfo;
import com.alibaba.alink.operator.local.lazy.ExtractModelInfoLocalOp;

import java.util.List;

public class VectorMaxAbsScalerModelInfoLocalOp extends
	ExtractModelInfoLocalOp <VectorMaxAbsScalarModelInfo, VectorMaxAbsScalerModelInfoLocalOp> {

	public VectorMaxAbsScalerModelInfoLocalOp() {
		this(null);
	}

	public VectorMaxAbsScalerModelInfoLocalOp(Params params) {
		super(params);
	}

	@Override
	protected VectorMaxAbsScalarModelInfo createModelInfo(List <Row> rows) {
		return new VectorMaxAbsScalarModelInfo(rows);
	}
}
