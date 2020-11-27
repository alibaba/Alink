package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.tree.TreeModelInfo;
import com.alibaba.alink.operator.common.tree.TreeModelInfo.RandomForestModelInfo;
import com.alibaba.alink.operator.common.tree.TreeModelInfoBatchOp;

import java.util.List;

public class RandomForestRegModelInfoBatchOp
	extends TreeModelInfoBatchOp<TreeModelInfo.RandomForestModelInfo, RandomForestRegModelInfoBatchOp> {

	private static final long serialVersionUID = 1954066538289103724L;

	public RandomForestRegModelInfoBatchOp() {
	}

	public RandomForestRegModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected RandomForestModelInfo createModelInfo(List <Row> rows) {
		return new RandomForestModelInfo(rows);
	}
}
