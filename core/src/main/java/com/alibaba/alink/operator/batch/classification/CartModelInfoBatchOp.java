package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.tree.TreeModelInfo.DecisionTreeModelInfo;
import com.alibaba.alink.operator.common.tree.TreeModelInfoBatchOp;

import java.util.List;

/**
 * The batch operator for model info of the cart model.
 */
public final class CartModelInfoBatchOp
	extends TreeModelInfoBatchOp <DecisionTreeModelInfo, CartModelInfoBatchOp> {

	private static final long serialVersionUID = 2584656676675616500L;

	public CartModelInfoBatchOp() {
	}

	public CartModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected DecisionTreeModelInfo createModelInfo(List <Row> rows) {
		return new DecisionTreeModelInfo(rows);
	}
}
