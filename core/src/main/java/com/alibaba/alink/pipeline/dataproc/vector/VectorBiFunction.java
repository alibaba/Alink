package com.alibaba.alink.pipeline.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorBiFunctionMapper;
import com.alibaba.alink.params.dataproc.vector.VectorBiFunctionParams;
import com.alibaba.alink.pipeline.MapTransformer;

public class VectorBiFunction extends MapTransformer <VectorBiFunction>
	implements VectorBiFunctionParams <VectorBiFunction> {

	private static final long serialVersionUID = 1611713161062319302L;

	public VectorBiFunction() {
		this(null);
	}

	public VectorBiFunction(Params param) {
		super(VectorBiFunctionMapper::new, param);
	}

}

