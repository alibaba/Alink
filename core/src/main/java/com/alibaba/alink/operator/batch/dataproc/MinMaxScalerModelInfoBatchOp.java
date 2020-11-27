package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.dataproc.MinMaxScalerModelInfo;

import java.util.List;

public class MinMaxScalerModelInfoBatchOp
	extends ExtractModelInfoBatchOp <MinMaxScalerModelInfo, MinMaxScalerModelInfoBatchOp> {

	private static final long serialVersionUID = 1002158987998750553L;

	public MinMaxScalerModelInfoBatchOp() {
		this(null);
	}

	public MinMaxScalerModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected MinMaxScalerModelInfo createModelInfo(List <Row> rows) {
		return new MinMaxScalerModelInfo(rows);
	}
}
