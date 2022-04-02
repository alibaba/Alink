package com.alibaba.alink.operator.stream.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.vector.VectorSliceMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.vector.VectorSliceParams;

/**
 * VectorSlicer is a transformer that takes a feature vector and outputs a new feature vector with a sub-array of the
 * original features. It is useful for extracting features from a vector column.
 */
@ParamSelectColumnSpec(name = "selectedCol", portIndices = 0, allowedTypeCollections = {TypeCollections.VECTOR_TYPES})
@NameCn("向量切片")
public final class VectorSliceStreamOp extends MapStreamOp <VectorSliceStreamOp>
	implements VectorSliceParams <VectorSliceStreamOp> {

	private static final long serialVersionUID = -3032334863737484545L;

	public VectorSliceStreamOp() {
		this(null);
	}

	public VectorSliceStreamOp(Params params) {
		super(VectorSliceMapper::new, params);
	}

}
