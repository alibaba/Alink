package com.alibaba.alink.operator.local.utils;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.operator.common.dataproc.vector.VectorSerializeMapper;

@Internal
public final class VectorSerializeLocalOp extends MapLocalOp <VectorSerializeLocalOp> {

	public VectorSerializeLocalOp() {
		this(null);
	}

	public VectorSerializeLocalOp(Params params) {
		super(VectorSerializeMapper::new, params);
	}
}
