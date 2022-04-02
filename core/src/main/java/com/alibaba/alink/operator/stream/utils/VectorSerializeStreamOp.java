package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.operator.common.dataproc.vector.VectorSerializeMapper;

@Internal
public final class VectorSerializeStreamOp extends MapStreamOp <VectorSerializeStreamOp> {

	private static final long serialVersionUID = -1331089574809127157L;

	public VectorSerializeStreamOp() {
		this(null);
	}

	public VectorSerializeStreamOp(Params params) {
		super(VectorSerializeMapper::new, params);
	}
}
