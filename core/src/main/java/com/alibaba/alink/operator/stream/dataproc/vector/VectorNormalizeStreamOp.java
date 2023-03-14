package com.alibaba.alink.operator.stream.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.vector.VectorNormalizeMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.vector.VectorNormalizeParams;

/**
 * Normalizer is a Transformer which transforms a dataset of Vector rows, normalizing each Vector to have unit norm. It
 * takes parameter p, which specifies the p-norm used for normalization. This normalization can help standardize your
 * input data and improve the behavior of learning algorithms.
 */
@ParamSelectColumnSpec(name = "selectedCol", portIndices = 0, allowedTypeCollections = {TypeCollections.VECTOR_TYPES})
@NameCn("向量标准化")
@NameEn("Vector normalize")
public final class VectorNormalizeStreamOp extends MapStreamOp <VectorNormalizeStreamOp>
	implements VectorNormalizeParams <VectorNormalizeStreamOp> {

	private static final long serialVersionUID = -2793008097390444539L;

	public VectorNormalizeStreamOp() {
		this(null);
	}

	public VectorNormalizeStreamOp(Params params) {
		super(VectorNormalizeMapper::new, params);
	}
}
