package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.dataproc.vector.VectorStandardScalerModelInfo;

import java.util.List;

public class VectorStandardScalerModelInfoBatchOp
	extends ExtractModelInfoBatchOp <VectorStandardScalerModelInfo, VectorStandardScalerModelInfoBatchOp> {

	private static final long serialVersionUID = 635830032335675104L;

	public VectorStandardScalerModelInfoBatchOp() {
		this(null);
	}

	public VectorStandardScalerModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected VectorStandardScalerModelInfo createModelInfo(List <Row> rows) {
		return new VectorStandardScalerModelInfo(rows);
	}
}
