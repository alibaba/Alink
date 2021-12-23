package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.MTableSerializeMapper;

@Internal
public final class MTableSerializeBatchOp extends MapBatchOp <MTableSerializeBatchOp> {

	private static final long serialVersionUID = 7868363049236528925L;

	public MTableSerializeBatchOp() {
		this(null);
	}

	public MTableSerializeBatchOp(Params params) {
		super(MTableSerializeMapper::new, params);
	}
}
