package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.vector.VectorElementwiseProductMapper;
import com.alibaba.alink.params.dataproc.vector.VectorElementwiseProductParams;

/**
 * VectorEleWiseProduct multiplies each input vector by a provided “scaling” vector, using element-wise multiplication.
 * In other words, it scales each column of the dataset by a scalar multiplier. This represents the Hadamard product
 * between the input vector, v and transforming vector, w, to yield a result vector.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量元素依次相乘")
@NameEn("Vector Elementwise Product")
public final class VectorElementwiseProductBatchOp extends MapBatchOp <VectorElementwiseProductBatchOp>
	implements VectorElementwiseProductParams <VectorElementwiseProductBatchOp> {

	private static final long serialVersionUID = -508063650510821861L;

	public VectorElementwiseProductBatchOp() {
		this(null);
	}

	public VectorElementwiseProductBatchOp(Params params) {
		super(VectorElementwiseProductMapper::new, params);
	}
}
