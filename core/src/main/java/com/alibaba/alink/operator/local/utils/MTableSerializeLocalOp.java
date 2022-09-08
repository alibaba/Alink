package com.alibaba.alink.operator.local.utils;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.operator.common.dataproc.vector.MTableSerializeMapper;

@Internal
public final class MTableSerializeLocalOp extends MapLocalOp <MTableSerializeLocalOp> {

	public MTableSerializeLocalOp() {
		this(null);
	}

	public MTableSerializeLocalOp(Params params) {
		super(MTableSerializeMapper::new, params);
	}
}
