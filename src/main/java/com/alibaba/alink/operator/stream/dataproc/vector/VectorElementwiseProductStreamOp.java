package com.alibaba.alink.operator.stream.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorElementwiseProductMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.vector.VectorElementwiseProductParams;

/**
 * VectorEleWiseProduct multiplies each input vector by a provided “scaling” vector, using element-wise multiplication.
 * In other words, it scales each column of the dataset by a scalar multiplier. This represents the Hadamard product
 * between the input vector, v and transforming vector, w, to yield a result vector.
 *
 */
public final class VectorElementwiseProductStreamOp extends MapStreamOp <VectorElementwiseProductStreamOp>
	implements VectorElementwiseProductParams <VectorElementwiseProductStreamOp> {

	public VectorElementwiseProductStreamOp(Params params) {
		super(VectorElementwiseProductMapper::new, params);

	}

	public VectorElementwiseProductStreamOp() {
		this(null);
	}
}
