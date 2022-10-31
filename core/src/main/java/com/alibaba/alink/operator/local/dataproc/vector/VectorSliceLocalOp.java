package com.alibaba.alink.operator.local.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.vector.VectorSliceMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.dataproc.vector.VectorSliceParams;

/**
 * VectorSlicer is a transformer that takes a feature vector and outputs a new feature vector with a sub-array of the
 * original features. It is useful for extracting features from a vector column.
 */
@NameCn("向量切片")
public final class VectorSliceLocalOp extends MapLocalOp <VectorSliceLocalOp>
	implements VectorSliceParams <VectorSliceLocalOp> {

	public VectorSliceLocalOp() {
		this(null);
	}

	public VectorSliceLocalOp(Params params) {
		super(VectorSliceMapper::new, params);
	}
}
