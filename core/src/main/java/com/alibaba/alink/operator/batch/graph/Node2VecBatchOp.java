package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.huge.impl.Node2VecImpl;

@NameCn("Node To Vector")
public class Node2VecBatchOp extends Node2VecImpl <Node2VecBatchOp> {

	private static final long serialVersionUID = 8596107700297808776L;

	public Node2VecBatchOp() {
		super(null);
	}

	public Node2VecBatchOp(Params params) {
		super(params);
	}
}
