package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.tree.TreeModelInfo;
import com.alibaba.alink.operator.common.tree.TreeModelInfo.DecisionTreeModelInfo;
import com.alibaba.alink.operator.common.tree.TreeModelInfoBatchOp;

import java.util.List;

public final class DecisionTreeRegModelInfoBatchOp
	extends TreeModelInfoBatchOp<TreeModelInfo.DecisionTreeModelInfo, DecisionTreeRegModelInfoBatchOp> {

	private static final long serialVersionUID = -7537536379615918213L;

	public DecisionTreeRegModelInfoBatchOp() {
	}

	public DecisionTreeRegModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected DecisionTreeModelInfo createModelInfo(List <Row> rows) {
		return new DecisionTreeModelInfo(rows);
	}
}
