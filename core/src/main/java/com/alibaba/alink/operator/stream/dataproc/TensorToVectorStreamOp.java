package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.TensorToVectorMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.TensorToVectorParams;

/**
 * stream op for tensor to vector.
 */
public class TensorToVectorStreamOp extends MapStreamOp <TensorToVectorStreamOp>
	implements TensorToVectorParams <TensorToVectorStreamOp> {

	public TensorToVectorStreamOp() {
		this(new Params());
	}

	public TensorToVectorStreamOp(Params params) {
		super(TensorToVectorMapper::new, params);
	}

}
