package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.tree.TreeModelInfo.DecisionTreeModelInfo;
import com.alibaba.alink.operator.common.tree.TreeModelInfoBatchOp;

import java.util.List;

/**
 * The batch operator for model info of the cart regression model.
 */
public final class CartRegModelInfoBatchOp
	extends TreeModelInfoBatchOp <DecisionTreeModelInfo, CartRegModelInfoBatchOp> {

	private static final long serialVersionUID = 1013761636249264154L;

	public CartRegModelInfoBatchOp() {
	}

	public CartRegModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected DecisionTreeModelInfo createModelInfo(List <Row> rows) {
		return new DecisionTreeModelInfo(rows);
	}
}
