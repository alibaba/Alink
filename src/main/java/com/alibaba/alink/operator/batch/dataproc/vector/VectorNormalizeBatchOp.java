package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorNormalizeMapper;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.params.dataproc.vector.VectorNormalizeParams;

/**
 * Normalizer is a Transformer which transforms a dataset of Vector rows, normalizing each Vector to have unit norm. It
 * takes parameter p, which specifies the p-norm used for normalization. This normalization can help standardize your
 * input data and improve the behavior of learning algorithms.
 */
public final class VectorNormalizeBatchOp extends MapBatchOp <VectorNormalizeBatchOp>
	implements VectorNormalizeParams <VectorNormalizeBatchOp> {

	public VectorNormalizeBatchOp() {
		this(null);
	}

	public VectorNormalizeBatchOp(Params params) {
		super(VectorNormalizeMapper::new, params);
	}
}
