package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.huge.impl.DeepWalkImpl;

@NameCn("DeepWalk")
public class DeepWalkBatchOp extends DeepWalkImpl <DeepWalkBatchOp> {
	private static final long serialVersionUID = -8007362121261574268L;

	public DeepWalkBatchOp(Params params) {
		super(params);
	}

	public DeepWalkBatchOp() {
		super(null);
	}
}
