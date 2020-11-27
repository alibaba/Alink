package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;

import java.util.List;

public class LdaModelInfoBatchOp extends ExtractModelInfoBatchOp <LdaModelInfo, LdaModelInfoBatchOp> {

	private static final long serialVersionUID = 8018775860830027877L;

	public LdaModelInfoBatchOp() {
		this(null);
	}

	public LdaModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected LdaModelInfo createModelInfo(List <Row> rows) {
		return new LdaModelInfo(rows);
	}
}
