package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.tree.TreeModelInfo;
import com.alibaba.alink.operator.common.tree.TreeModelInfo.DecisionTreeModelInfo;
import com.alibaba.alink.operator.common.tree.TreeModelInfoBatchOp;

import java.util.List;

public final class DecisionTreeModelInfoBatchOp
	extends TreeModelInfoBatchOp<TreeModelInfo.DecisionTreeModelInfo, DecisionTreeModelInfoBatchOp> {

	private static final long serialVersionUID = 1245663316324567010L;

	public DecisionTreeModelInfoBatchOp() {
	}

	public DecisionTreeModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected DecisionTreeModelInfo createModelInfo(List <Row> rows) {
		return new DecisionTreeModelInfo(rows);
	}
}
