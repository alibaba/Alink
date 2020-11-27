package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorSerializeMapper;

public final class VectorSerializeBatchOp extends MapBatchOp <VectorSerializeBatchOp> {

	private static final long serialVersionUID = 7868363049236528925L;

	public VectorSerializeBatchOp() {
		this(null);
	}

	public VectorSerializeBatchOp(Params params) {
		super(VectorSerializeMapper::new, params);
	}
}
