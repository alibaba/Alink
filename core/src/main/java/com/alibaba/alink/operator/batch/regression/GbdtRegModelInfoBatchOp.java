package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.tree.TreeModelInfo;
import com.alibaba.alink.operator.common.tree.TreeModelInfo.GbdtModelInfo;
import com.alibaba.alink.operator.common.tree.TreeModelInfoBatchOp;

import java.util.List;

public class GbdtRegModelInfoBatchOp
	extends TreeModelInfoBatchOp<TreeModelInfo.GbdtModelInfo, GbdtRegModelInfoBatchOp> {

	private static final long serialVersionUID = -830323093088396568L;

	public GbdtRegModelInfoBatchOp() {
	}

	public GbdtRegModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected GbdtModelInfo createModelInfo(List <Row> rows) {
		return new GbdtModelInfo(rows);
	}
}
