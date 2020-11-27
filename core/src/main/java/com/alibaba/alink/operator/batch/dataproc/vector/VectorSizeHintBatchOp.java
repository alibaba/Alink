package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.vector.VectorSizeHintMapper;
import com.alibaba.alink.params.dataproc.vector.VectorSizeHintParams;

/**
 * Check the size of a vector. if size is not match, then do as handleInvalid.
 * If error, will throw exception if the vector is null or the vector size doesn't match the given one.
 * If optimistic, will accept the vector if it is not null.
 */
public final class VectorSizeHintBatchOp extends MapBatchOp <VectorSizeHintBatchOp>
	implements VectorSizeHintParams <VectorSizeHintBatchOp> {

	private static final long serialVersionUID = -4289050155795324155L;

	/**
	 * handleInvalid can be "error", "optimistic"
	 */
	public VectorSizeHintBatchOp() {
		this(null);
	}

	public VectorSizeHintBatchOp(Params params) {
		super(VectorSizeHintMapper::new, params);
	}
}
