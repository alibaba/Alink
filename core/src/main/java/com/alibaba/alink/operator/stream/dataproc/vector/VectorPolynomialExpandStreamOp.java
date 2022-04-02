package com.alibaba.alink.operator.stream.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.vector.PolynomialExpansionMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.vector.VectorPolynomialExpandParams;

/**
 * Polynomial expansion is the process of expanding your features into a polynomial space, which is formulated by an
 * n-degree combination of original dimensions. Take a 2-variable feature vector as an example: (x, y), if we want to
 * expand it with degree 2, then we get (x, x * x, y, x * y, y * y).
 */
@ParamSelectColumnSpec(name = "selectedCol", portIndices = 0, allowedTypeCollections = {TypeCollections.VECTOR_TYPES})
@NameCn("向量多项式展开")
public final class VectorPolynomialExpandStreamOp extends MapStreamOp <VectorPolynomialExpandStreamOp>
	implements VectorPolynomialExpandParams <VectorPolynomialExpandStreamOp> {

	private static final long serialVersionUID = 5914061375545533619L;

	public VectorPolynomialExpandStreamOp(Params params) {
		super(PolynomialExpansionMapper::new, params);
	}

	public VectorPolynomialExpandStreamOp() {
		this(null);
	}
}
