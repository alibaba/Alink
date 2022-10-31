package com.alibaba.alink.operator.local.dataproc;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.dataproc.StandardScalerModelInfo;
import com.alibaba.alink.operator.local.lazy.ExtractModelInfoLocalOp;

import java.util.List;

public class StandardScalerModelInfoLocalOp extends
	ExtractModelInfoLocalOp <StandardScalerModelInfo, StandardScalerModelInfoLocalOp> {

	public StandardScalerModelInfoLocalOp() {
		this(null);
	}

	public StandardScalerModelInfoLocalOp(Params params) {
		super(params);
	}

	@Override
	protected StandardScalerModelInfo createModelInfo(List <Row> rows) {
		return new StandardScalerModelInfo(rows);
	}
}
