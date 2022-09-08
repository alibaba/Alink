package com.alibaba.alink.operator.local.utils;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.operator.common.dataproc.TensorSerializeMapper;

@Internal
public final class TensorSerializeLocalOp extends MapLocalOp <TensorSerializeLocalOp> {

	public TensorSerializeLocalOp() {
		this(null);
	}

	public TensorSerializeLocalOp(Params params) {
		super(TensorSerializeMapper::new, params);
	}
}
