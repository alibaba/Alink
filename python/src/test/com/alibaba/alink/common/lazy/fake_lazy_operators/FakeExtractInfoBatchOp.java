package com.alibaba.alink.common.lazy.fake_lazy_operators;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;

import java.util.List;

public class FakeExtractInfoBatchOp extends ExtractModelInfoBatchOp <FakeModelInfo, FakeExtractInfoBatchOp> {

	public FakeExtractInfoBatchOp() {
		super(null);
	}

	public FakeExtractInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected FakeModelInfo createModelInfo(List <Row> rows) {
		return new FakeModelInfo(rows);
	}
}
