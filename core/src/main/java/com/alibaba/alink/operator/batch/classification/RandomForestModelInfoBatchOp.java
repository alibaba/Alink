package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.tree.TreeModelInfo;
import com.alibaba.alink.operator.common.tree.TreeModelInfo.RandomForestModelInfo;
import com.alibaba.alink.operator.common.tree.TreeModelInfoBatchOp;

import java.util.List;

public class RandomForestModelInfoBatchOp
	extends TreeModelInfoBatchOp<TreeModelInfo.RandomForestModelInfo, RandomForestModelInfoBatchOp> {

	private static final long serialVersionUID = -2739115521492797450L;

	public RandomForestModelInfoBatchOp() {
	}

	public RandomForestModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected RandomForestModelInfo createModelInfo(List <Row> rows) {
		return new RandomForestModelInfo(rows);
	}
}
