package com.alibaba.alink.operator.local.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.vector.VectorNormalizeMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.dataproc.vector.VectorNormalizeParams;

/**
 * Normalizer is a Transformer which transforms a dataset of Vector rows, normalizing each Vector to have unit norm. It
 * takes parameter p, which specifies the p-norm used for normalization. This normalization can help standardize your
 * input data and improve the behavior of learning algorithms.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量标准化")
public final class VectorNormalizeLocalOp extends MapLocalOp <VectorNormalizeLocalOp>
	implements VectorNormalizeParams <VectorNormalizeLocalOp> {

	public VectorNormalizeLocalOp() {
		this(null);
	}

	public VectorNormalizeLocalOp(Params params) {
		super(VectorNormalizeMapper::new, params);
	}
}
