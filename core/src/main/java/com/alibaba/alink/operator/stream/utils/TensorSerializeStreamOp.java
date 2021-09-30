package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.tensor.TensorSerializeMapper;

public final class TensorSerializeStreamOp extends MapStreamOp <TensorSerializeStreamOp> {

	private static final long serialVersionUID = -1331089574809127157L;

	public TensorSerializeStreamOp() {
		this(null);
	}

	public TensorSerializeStreamOp(Params params) {
		super(TensorSerializeMapper::new, params);
	}
}
