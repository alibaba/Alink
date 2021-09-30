package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.tensor.TensorToVectorMapper;
import com.alibaba.alink.params.dataproc.TensorToVectorParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * tensor to vector.
 */
public class TensorToVector extends MapTransformer <TensorToVector>
	implements TensorToVectorParams <TensorToVector> {

	public TensorToVector() {
		this(new Params());
	}

	public TensorToVector(Params params) {
		super(TensorToVectorMapper::new, params);
	}

}
