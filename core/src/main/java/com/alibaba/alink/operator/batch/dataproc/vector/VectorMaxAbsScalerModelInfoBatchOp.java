package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.dataproc.vector.VectorMaxAbsScalarModelInfo;

import java.util.List;

public class VectorMaxAbsScalerModelInfoBatchOp extends
	ExtractModelInfoBatchOp <VectorMaxAbsScalarModelInfo, VectorMaxAbsScalerModelInfoBatchOp> {

	private static final long serialVersionUID = -4176922579380263805L;

	public VectorMaxAbsScalerModelInfoBatchOp() {
		this(null);
	}

	public VectorMaxAbsScalerModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected VectorMaxAbsScalarModelInfo createModelInfo(List <Row> rows) {
		return new VectorMaxAbsScalarModelInfo(rows);
	}
}
