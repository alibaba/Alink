package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.utils.SideOutputParams;

@SuppressWarnings("unused")
public class SideOutputBatchOp extends BatchOperator <SideOutputBatchOp>
	implements SideOutputParams <SideOutputBatchOp> {

	public SideOutputBatchOp() {
	}

	public SideOutputBatchOp(Params params) {
		super(params);
	}

	@Override
	public SideOutputBatchOp linkFrom(BatchOperator <?>... inputs) {
		int index = getIndex();
		BatchOperator <?> in = checkAndGetFirst(inputs);
		setOutputTable(in.getSideOutput(index).getOutputTable());
		return this;
	}
}
