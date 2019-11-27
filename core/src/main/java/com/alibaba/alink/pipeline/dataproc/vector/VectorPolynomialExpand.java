package com.alibaba.alink.pipeline.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.PolynomialExpansionMapper;
import com.alibaba.alink.params.dataproc.vector.VectorPolynomialExpandParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * Polynomial expansion is the process of expanding your features into a polynomial space, which is formulated by an
 * n-degree combination of original dimensions. Take a 2-variable feature vector as an example: (x, y), if we want to
 * expand it with degree 2, then we get (x, x * x, y, x * y, y * y).
 */
public class VectorPolynomialExpand extends MapTransformer<VectorPolynomialExpand>
	implements VectorPolynomialExpandParams <VectorPolynomialExpand> {

	public VectorPolynomialExpand() {
		this(null);
	}

	public VectorPolynomialExpand(Params params) {
		super(PolynomialExpansionMapper::new, params);
	}
}
