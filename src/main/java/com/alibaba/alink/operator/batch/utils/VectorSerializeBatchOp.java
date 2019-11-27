package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorSerializeMapper;

public final class VectorSerializeBatchOp extends MapBatchOp <VectorSerializeBatchOp> {

	public VectorSerializeBatchOp() {
		this(null);
	}

	public VectorSerializeBatchOp(Params params) {
		super(VectorSerializeMapper::new, params);
	}
}
