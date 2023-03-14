package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.utils.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.regression.glm.GlmModelInfo;

import java.util.List;

public class GlmModelInfoBatchOp
	extends ExtractModelInfoBatchOp <GlmModelInfo, GlmModelInfoBatchOp> {

	private static final long serialVersionUID = -4827765586949128150L;

	public GlmModelInfoBatchOp() {
		this(null);
	}

	public GlmModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected GlmModelInfo createModelInfo(List <Row> rows) {
		return new GlmModelInfo(rows);
	}

}
