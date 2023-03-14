package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.vector.VectorSliceMapper;
import com.alibaba.alink.params.dataproc.vector.VectorSliceParams;

/**
 * VectorSlicer is a transformer that takes a feature vector and outputs a new feature vector with a sub-array of the
 * original features. It is useful for extracting features from a vector column.
 */
@NameCn("向量切片")
@NameEn("Vector Slice")
public final class VectorSliceBatchOp extends MapBatchOp <VectorSliceBatchOp>
	implements VectorSliceParams <VectorSliceBatchOp> {

	private static final long serialVersionUID = 1618905519246569791L;

	public VectorSliceBatchOp() {
		this(null);
	}

	public VectorSliceBatchOp(Params params) {
		super(VectorSliceMapper::new, params);
	}
}
