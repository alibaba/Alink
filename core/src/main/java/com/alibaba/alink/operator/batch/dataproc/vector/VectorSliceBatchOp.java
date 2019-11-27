package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorSliceMapper;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.params.dataproc.vector.VectorSliceParams;

/**
 * VectorSlicer is a transformer that takes a feature vector and outputs a new feature vector with a sub-array of the
 * original features. It is useful for extracting features from a vector column.
 */
public final class VectorSliceBatchOp extends MapBatchOp <VectorSliceBatchOp>
	implements VectorSliceParams <VectorSliceBatchOp> {

	public VectorSliceBatchOp() {
		this(null);
	}

	public VectorSliceBatchOp(Params params) {
		super(VectorSliceMapper::new, params);
	}
}
