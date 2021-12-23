package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.VectorToTensorMapper;
import com.alibaba.alink.params.dataproc.VectorToTensorParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * vector to tensor.
 */
public class VectorToTensor extends MapTransformer <VectorToTensor>
	implements VectorToTensorParams <VectorToTensor> {

	public VectorToTensor() {
		this(new Params());
	}

	public VectorToTensor(Params params) {
		super(VectorToTensorMapper::new, params);
	}

}
