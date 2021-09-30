package com.alibaba.alink.operator.batch.dataproc.tensor;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.tensor.TensorToVectorMapper;
import com.alibaba.alink.params.dataproc.TensorToVectorParams;

/**
 * batch op for tensor to vector.
 */
public class TensorToVectorBatchOp extends MapBatchOp <TensorToVectorBatchOp>
	implements TensorToVectorParams <TensorToVectorBatchOp> {

	public TensorToVectorBatchOp() {
		this(new Params());
	}

	public TensorToVectorBatchOp(Params params) {
		super(TensorToVectorMapper::new, params);
	}

}
