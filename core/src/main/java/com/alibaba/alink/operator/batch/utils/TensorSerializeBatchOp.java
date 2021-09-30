package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.tensor.TensorSerializeMapper;

public final class TensorSerializeBatchOp extends MapBatchOp <TensorSerializeBatchOp> {

	private static final long serialVersionUID = 7868363049236528925L;

	public TensorSerializeBatchOp() {
		this(null);
	}

	public TensorSerializeBatchOp(Params params) {
		super(TensorSerializeMapper::new, params);
	}
}
