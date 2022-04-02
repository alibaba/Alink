package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.huge.impl.MetaPath2VecImpl;

@NameCn("MetaPath To Vector")
public class MetaPath2VecBatchOp extends MetaPath2VecImpl <MetaPath2VecBatchOp> {
	private static final long serialVersionUID = -6118527393338279346L;

	public MetaPath2VecBatchOp() {
		this(null);
	}

	public MetaPath2VecBatchOp(Params params) {
		super(params);
	}
}
