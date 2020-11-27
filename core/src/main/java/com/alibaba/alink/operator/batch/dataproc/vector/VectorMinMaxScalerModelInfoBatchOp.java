package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.dataproc.vector.VectorMinMaxScalerModelInfo;

import java.util.List;

public class VectorMinMaxScalerModelInfoBatchOp
	extends ExtractModelInfoBatchOp <VectorMinMaxScalerModelInfo, VectorMinMaxScalerModelInfoBatchOp> {

	private static final long serialVersionUID = 3486899171037641375L;

	public VectorMinMaxScalerModelInfoBatchOp() {
		this(null);
	}

	public VectorMinMaxScalerModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected VectorMinMaxScalerModelInfo createModelInfo(List <Row> rows) {
		return new VectorMinMaxScalerModelInfo(rows);
	}
}
