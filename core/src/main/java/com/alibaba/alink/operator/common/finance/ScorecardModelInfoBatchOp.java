package com.alibaba.alink.operator.common.finance;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.utils.ExtractModelInfoBatchOp;

import java.util.List;

public class ScorecardModelInfoBatchOp extends ExtractModelInfoBatchOp <ScorecardModelInfo, ScorecardModelInfoBatchOp> {
	private static final long serialVersionUID = 1735133462550836751L;

	public ScorecardModelInfoBatchOp() {
		this(null);
	}

	public ScorecardModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	public ScorecardModelInfo createModelInfo(List <Row> rows) {
		return new ScorecardModelInfo(rows, this.getSchema());
	}
}
