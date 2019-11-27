package com.alibaba.alink.operator.stream.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorSizeHintMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.vector.VectorSizeHintParams;

/**
 * Check the size of a vector. if size is not match, then do as handleInvalid
 *
 */
public final class VectorSizeHintStreamOp extends MapStreamOp <VectorSizeHintStreamOp>
	implements VectorSizeHintParams <VectorSizeHintStreamOp> {

	/**
	 * handleInvalidMethod can be "error", "skip", "optimistic"
	 */
	public VectorSizeHintStreamOp() {
		this(null);
	}

	public VectorSizeHintStreamOp(Params params) {
		super(VectorSizeHintMapper::new, params);
	}
}
