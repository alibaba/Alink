package com.alibaba.alink.operator.local.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.vector.VectorElementwiseProductMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.dataproc.vector.VectorElementwiseProductParams;

/**
 * VectorEleWiseProduct multiplies each input vector by a provided “scaling” vector, using element-wise multiplication.
 * In other words, it scales each column of the dataset by a scalar multiplier. This represents the Hadamard product
 * between the input vector, v and transforming vector, w, to yield a result vector.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量元素依次相乘")
public final class VectorElementwiseProductLocalOp extends MapLocalOp <VectorElementwiseProductLocalOp>
	implements VectorElementwiseProductParams <VectorElementwiseProductLocalOp> {

	public VectorElementwiseProductLocalOp() {
		this(null);
	}

	public VectorElementwiseProductLocalOp(Params params) {
		super(VectorElementwiseProductMapper::new, params);
	}
}
