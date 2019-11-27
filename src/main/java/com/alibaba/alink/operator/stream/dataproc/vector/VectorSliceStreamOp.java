package com.alibaba.alink.operator.stream.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorSliceMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.vector.VectorSliceParams;

/**
 * VectorSlicer is a transformer that takes a feature vector and outputs a new feature vector with a sub-array of the
 * original features. It is useful for extracting features from a vector column.
 *
 */
public final class VectorSliceStreamOp extends MapStreamOp <VectorSliceStreamOp>
	implements VectorSliceParams <VectorSliceStreamOp> {

	public VectorSliceStreamOp() {
		this(null);
	}

	public VectorSliceStreamOp(Params params) {
		super(VectorSliceMapper::new, params);
	}

}
