package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.tree.TreeModelInfo.DecisionTreeModelInfo;
import com.alibaba.alink.operator.common.tree.TreeModelInfoBatchOp;

import java.util.List;


/**
 * The batch operator for model info of the id3 model.
 */
public final class Id3ModelInfoBatchOp
	extends TreeModelInfoBatchOp <DecisionTreeModelInfo, Id3ModelInfoBatchOp> {

	private static final long serialVersionUID = -2844454742604346704L;

	public Id3ModelInfoBatchOp() {
	}

	public Id3ModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected DecisionTreeModelInfo createModelInfo(List <Row> rows) {
		return new DecisionTreeModelInfo(rows);
	}
}
