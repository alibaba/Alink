package com.alibaba.alink.operator.local.dataproc;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.dataproc.MinMaxScalerModelInfo;
import com.alibaba.alink.operator.local.lazy.ExtractModelInfoLocalOp;

import java.util.List;

public class MinMaxScalerModelInfoLocalOp
	extends ExtractModelInfoLocalOp <MinMaxScalerModelInfo, MinMaxScalerModelInfoLocalOp> {

	public MinMaxScalerModelInfoLocalOp() {
		this(null);
	}

	public MinMaxScalerModelInfoLocalOp(Params params) {
		super(params);
	}

	@Override
	protected MinMaxScalerModelInfo createModelInfo(List <Row> rows) {
		return new MinMaxScalerModelInfo(rows);
	}
}
