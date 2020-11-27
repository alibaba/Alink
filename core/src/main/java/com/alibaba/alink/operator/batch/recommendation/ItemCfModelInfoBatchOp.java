package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.recommendation.ItemCfModelInfo;

import java.util.List;

public class ItemCfModelInfoBatchOp extends ExtractModelInfoBatchOp <ItemCfModelInfo, ItemCfModelInfoBatchOp> {
	private static final long serialVersionUID = 1735133462550836751L;

	public ItemCfModelInfoBatchOp() {
		this(null);
	}

	public ItemCfModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	public ItemCfModelInfo createModelInfo(List <Row> rows) {
		return new ItemCfModelInfo(rows);
	}
}
