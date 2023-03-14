package com.alibaba.alink.params.shared.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMinPoints<T> extends WithParams <T> {
	@NameCn("邻域中样本个数的阈值")
	@DescCn("邻域中样本个数的阈值")
	ParamInfo <Integer> MIN_POINTS = ParamInfoFactory
		.createParamInfo("minPoints", Integer.class)
		.setDescription("min points")
		.setRequired()
		.build();

	default Integer getMinPoints() {return get(MIN_POINTS);}

	default T setMinPoints(Integer value) {return set(MIN_POINTS, value);}
}
