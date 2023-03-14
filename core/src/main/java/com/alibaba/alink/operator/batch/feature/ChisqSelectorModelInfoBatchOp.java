package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.utils.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.feature.ChisqSelectorModelInfo;

import java.util.List;

public class ChisqSelectorModelInfoBatchOp
	extends ExtractModelInfoBatchOp <ChisqSelectorModelInfo, ChisqSelectorModelInfoBatchOp> {

	private static final long serialVersionUID = 1296031497464809813L;

	public ChisqSelectorModelInfoBatchOp() {
		this(null);
	}

	public ChisqSelectorModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected ChisqSelectorModelInfo createModelInfo(List <Row> rows) {
		return new ChisqSelectorModelInfo(rows);
	}
}
