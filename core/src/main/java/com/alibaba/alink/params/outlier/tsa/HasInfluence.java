package com.alibaba.alink.params.outlier.tsa;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.validators.RangeValidator;

public interface HasInfluence<T> extends WithParams <T> {

	@NameCn("影响度")
	@DescCn("异常点对均值/方差计算的影响系数")
	ParamInfo <Double> INFLUENCE = ParamInfoFactory
		.createParamInfo("influence", Double.class)
		.setDescription("influence")
		.setHasDefaultValue(0.5)
		.setValidator(new RangeValidator <>(0.0, 1.0))
		.build();

	default Double getInfluence() {return get(INFLUENCE);}

	default T setInfluence(Double value) {return set(INFLUENCE, value);}
}
