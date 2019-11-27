package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorSerializeMapper;

public final class VectorSerializeStreamOp extends MapStreamOp <VectorSerializeStreamOp> {

	public VectorSerializeStreamOp() {
		this(null);
	}

	public VectorSerializeStreamOp(Params params) {
		super(VectorSerializeMapper::new, params);
	}
}
