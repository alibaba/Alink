package com.alibaba.alink.pipeline.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorNormalizeMapper;
import com.alibaba.alink.params.dataproc.vector.VectorNormalizeParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * Normalizer is a Transformer which transforms a dataset of Vector rows, normalizing each Vector to have unit norm. It
 * takes parameter p, which specifies the p-norm used for normalization. This normalization can help standardize your
 * input data and improve the behavior of learning algorithms.
 */
public class VectorNormalizer extends MapTransformer<VectorNormalizer>
	implements VectorNormalizeParams <VectorNormalizer> {

	public VectorNormalizer() {
		this(null);
	}

	public VectorNormalizer(Params params) {
		super(VectorNormalizeMapper::new, params);
	}
}