package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.utils.SideOutputParams;

@SuppressWarnings("unused")
public class SideOutputStreamOp extends StreamOperator <SideOutputStreamOp>
	implements SideOutputParams <SideOutputStreamOp> {

	public SideOutputStreamOp() {
	}

	public SideOutputStreamOp(Params params) {
		super(params);
	}

	@Override
	public SideOutputStreamOp linkFrom(StreamOperator <?>... inputs) {
		int index = getIndex();
		StreamOperator <?> streamOperator = checkAndGetFirst(inputs);
		StreamOperator <?> in = checkAndGetFirst(inputs);
		setOutputTable(in.getSideOutput(index).getOutputTable());
		return this;
	}
}
