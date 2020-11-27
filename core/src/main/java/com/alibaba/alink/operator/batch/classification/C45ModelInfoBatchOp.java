package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.tree.TreeModelInfo.DecisionTreeModelInfo;
import com.alibaba.alink.operator.common.tree.TreeModelInfoBatchOp;

import java.util.List;

/**
 * The batch operator for model info of the c45 model.
 */
public final class C45ModelInfoBatchOp
	extends TreeModelInfoBatchOp <DecisionTreeModelInfo, C45ModelInfoBatchOp> {

	private static final long serialVersionUID = 5353714482984782561L;

	public C45ModelInfoBatchOp() {
	}

	public C45ModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected DecisionTreeModelInfo createModelInfo(List <Row> rows) {
		return new DecisionTreeModelInfo(rows);
	}
}
