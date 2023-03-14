package com.alibaba.alink.operator.common.feature.binning;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.utils.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.feature.BinningModelInfo;

import java.util.List;

public class BinningModelInfoBatchOp
	extends ExtractModelInfoBatchOp <BinningModelInfo, BinningModelInfoBatchOp> {
	private static final long serialVersionUID = 1735133462550836751L;

	public BinningModelInfoBatchOp() {
		this(null);
	}

	public BinningModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	public BinningModelInfo createModelInfo(List <Row> rows) {
		return new BinningModelInfo(rows);
	}
}
