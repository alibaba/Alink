package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.vector.PolynomialExpansionMapper;
import com.alibaba.alink.params.dataproc.vector.VectorPolynomialExpandParams;

/**
 * Polynomial expansion is the process of expanding your features into a polynomial space, which is formulated by an
 * n-degree combination of original dimensions. Take a 2-variable feature vector as an example: (x, y), if we want to
 * expand it with degree 2, then we get (x, x * x, y, x * y, y * y).
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量多项式展开")
public final class VectorPolynomialExpandBatchOp extends MapBatchOp <VectorPolynomialExpandBatchOp>
	implements VectorPolynomialExpandParams <VectorPolynomialExpandBatchOp> {

	private static final long serialVersionUID = -4262039563742274130L;

	public VectorPolynomialExpandBatchOp() {
		this(null);
	}

	public VectorPolynomialExpandBatchOp(Params params) {
		super(PolynomialExpansionMapper::new, params);
	}
}
