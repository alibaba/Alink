package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.tree.TreeModelInfo;
import com.alibaba.alink.operator.common.tree.TreeModelInfo.GbdtModelInfo;
import com.alibaba.alink.operator.common.tree.TreeModelInfoBatchOp;

import java.util.List;

public class GbdtModelInfoBatchOp
	extends TreeModelInfoBatchOp<TreeModelInfo.GbdtModelInfo, GbdtModelInfoBatchOp> {
	private static final long serialVersionUID = -3480110970465880504L;

	public GbdtModelInfoBatchOp() {
	}

	public GbdtModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected GbdtModelInfo createModelInfo(List <Row> rows) {
		return new GbdtModelInfo(rows);
	}
}
