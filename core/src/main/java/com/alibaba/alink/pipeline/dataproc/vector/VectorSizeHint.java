package com.alibaba.alink.pipeline.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorSizeHintMapper;
import com.alibaba.alink.params.dataproc.vector.VectorSizeHintParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * Check the size of a vector. if size is not match, then do as handleInvalid.
 */
public class VectorSizeHint extends MapTransformer<VectorSizeHint>
	implements VectorSizeHintParams <VectorSizeHint> {

	public VectorSizeHint() {
		this(null);
	}

	public VectorSizeHint(Params params) {
		super(VectorSizeHintMapper::new, params);
	}
}
