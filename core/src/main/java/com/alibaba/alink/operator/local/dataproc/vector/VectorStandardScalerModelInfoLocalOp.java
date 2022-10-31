package com.alibaba.alink.operator.local.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.dataproc.vector.VectorStandardScalerModelInfo;
import com.alibaba.alink.operator.local.lazy.ExtractModelInfoLocalOp;

import java.util.List;

public class VectorStandardScalerModelInfoLocalOp
	extends ExtractModelInfoLocalOp <VectorStandardScalerModelInfo, VectorStandardScalerModelInfoLocalOp> {

	public VectorStandardScalerModelInfoLocalOp() {
		this(null);
	}

	public VectorStandardScalerModelInfoLocalOp(Params params) {
		super(params);
	}

	@Override
	protected VectorStandardScalerModelInfo createModelInfo(List <Row> rows) {
		return new VectorStandardScalerModelInfo(rows);
	}
}
