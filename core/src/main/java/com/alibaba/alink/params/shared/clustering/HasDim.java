package com.alibaba.alink.params.shared.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasDim<T> extends WithParams <T> {

	@NameCn("维度")
	@DescCn("维度")
	ParamInfo <Integer> DIM = ParamInfoFactory
		.createParamInfo("vectorSize", Integer.class)
		.setDescription("vectorSize")
		.setHasDefaultValue(null)
		.build();

	default Integer getDim() {return get(DIM);}

	default T setDim(Integer value) {return set(DIM, value);}
}
