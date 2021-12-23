package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.MTableSerializeMapper;

@Internal
public final class MTableSerializeStreamOp extends MapStreamOp <MTableSerializeStreamOp> {

	private static final long serialVersionUID = -1331089574809127157L;

	public MTableSerializeStreamOp() {
		this(null);
	}

	public MTableSerializeStreamOp(Params params) {
		super(MTableSerializeMapper::new, params);
	}
}
