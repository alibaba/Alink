package com.alibaba.alink.pipeline.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.vector.VectorFunctionMapper;
import com.alibaba.alink.params.dataproc.vector.VectorFunctionParams;
import com.alibaba.alink.pipeline.MapTransformer;

@NameCn("向量函数")
public class VectorFunction extends MapTransformer <VectorFunction>
	implements VectorFunctionParams <VectorFunction> {

	private static final long serialVersionUID = 1611703161062319302L;

	public VectorFunction() {
		this(null);
	}

	public VectorFunction(Params param) {
		super(VectorFunctionMapper::new, param);
	}

}

