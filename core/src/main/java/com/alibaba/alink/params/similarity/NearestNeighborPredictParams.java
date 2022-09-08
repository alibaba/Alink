package com.alibaba.alink.params.similarity;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.mapper.SISOModelMapperParams;
import com.alibaba.alink.params.validators.MinValidator;

/**
 * Parameters for nearest neighbor predict.
 */
public interface NearestNeighborPredictParams<T> extends SISOModelMapperParams <T> {

	@NameCn("radius值")
	@DescCn("radius值")
	ParamInfo <Double> RADIUS = ParamInfoFactory
		.createParamInfo("radius", Double.class)
		.setDescription("radius")
		.setHasDefaultValue(null)
		.build();

	default Double getRadius() {return get(RADIUS);}

	default T setRadius(Double value) {return set(RADIUS, value);}

	@NameCn("TopN的值")
	@DescCn("TopN的值")
	ParamInfo <Integer> TOP_N = ParamInfoFactory
		.createParamInfo("topN", Integer.class)
		.setDescription("top n")
		.setHasDefaultValue(null)
		.setValidator(new MinValidator <>(1).setNullValid(true))
		.build();

	default Integer getTopN() {
		return get(TOP_N);
	}

	default T setTopN(Integer value) {
		return set(TOP_N, value);
	}
}
