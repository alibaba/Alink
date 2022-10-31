package com.alibaba.alink.operator.local.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.dataproc.vector.VectorMinMaxScalerModelInfo;
import com.alibaba.alink.operator.local.lazy.ExtractModelInfoLocalOp;

import java.util.List;

public class VectorMinMaxScalerModelInfoLocalOp
	extends ExtractModelInfoLocalOp <VectorMinMaxScalerModelInfo, VectorMinMaxScalerModelInfoLocalOp> {

	public VectorMinMaxScalerModelInfoLocalOp() {
		this(null);
	}

	public VectorMinMaxScalerModelInfoLocalOp(Params params) {
		super(params);
	}

	@Override
	protected VectorMinMaxScalerModelInfo createModelInfo(List <Row> rows) {
		return new VectorMinMaxScalerModelInfo(rows);
	}
}
