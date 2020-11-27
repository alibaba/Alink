package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.dataproc.StandardScalerModelInfo;

import java.util.List;

public class StandardScalerModelInfoBatchOp extends
	ExtractModelInfoBatchOp <StandardScalerModelInfo, StandardScalerModelInfoBatchOp> {

	private static final long serialVersionUID = 5386170730354405968L;

	public StandardScalerModelInfoBatchOp() {
		this(null);
	}

	public StandardScalerModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected StandardScalerModelInfo createModelInfo(List <Row> rows) {
		return new StandardScalerModelInfo(rows);
	}
}
