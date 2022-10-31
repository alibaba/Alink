package com.alibaba.alink.params.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.shared.HasNumThreads;
import com.alibaba.alink.params.similarity.HasMaxNumCandidates;
import com.alibaba.alink.params.similarity.VectorLSHParams;
import com.alibaba.alink.params.similarity.VectorNearestNeighborTrainParams;
import com.alibaba.alink.params.validators.MinValidator;

public interface DbscanTrainParams<T> extends VectorNearestNeighborTrainParams <T>,
	VectorLSHParams <T>, HasMaxNumCandidates <T>, HasNumThreads <T> {

	@NameCn("radius值")
	@DescCn("radius值")
	ParamInfo <Double> RADIUS = ParamInfoFactory
		.createParamInfo("radius", Double.class)
		.setDescription("radius")
		.setRequired()
		.build();

	default Double getRadius() {return get(RADIUS);}

	default T setRadius(Double value) {return set(RADIUS, value);}

	@NameCn("TopN的值")
	@DescCn("TopN的值")
	ParamInfo <Integer> TOP_N = ParamInfoFactory
		.createParamInfo("topN", Integer.class)
		.setDescription("top n")
		.setRequired()
		.setValidator(new MinValidator <>(1).setNullValid(true))
		.build();

	default Integer getTopN() {
		return get(TOP_N);
	}

	default T setTopN(Integer value) {
		return set(TOP_N, value);
	}
}
