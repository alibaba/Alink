package com.alibaba.alink.operator.stream.dataproc.tensor;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.tensor.TensorToVectorMapper;
import com.alibaba.alink.params.dataproc.TensorToVectorParams;

/**
 * stream op for tensor to vector.
 */
public class TensorToVectorStreamOp extends MapBatchOp <TensorToVectorStreamOp>
	implements TensorToVectorParams <TensorToVectorStreamOp> {

	public TensorToVectorStreamOp() {
		this(new Params());
	}

	public TensorToVectorStreamOp(Params params) {
		super(TensorToVectorMapper::new, params);
	}

}
