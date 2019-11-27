package com.alibaba.alink.operator.stream.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorNormalizeMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.vector.VectorNormalizeParams;

/**
 * Normalizer is a Transformer which transforms a dataset of Vector rows, normalizing each Vector to have unit norm. It
 * takes parameter p, which specifies the p-norm used for normalization. This normalization can help standardize your
 * input data and improve the behavior of learning algorithms.
 *
 */
public final class VectorNormalizeStreamOp extends MapStreamOp <VectorNormalizeStreamOp>
	implements VectorNormalizeParams <VectorNormalizeStreamOp> {

	public VectorNormalizeStreamOp() {
		this(null);
	}

	public VectorNormalizeStreamOp(Params params) {
		super(VectorNormalizeMapper::new, params);
	}
}
