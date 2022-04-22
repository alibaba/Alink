package com.alibaba.alink.params.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.shared.clustering.HasFastDistanceType;
import com.alibaba.alink.params.validators.MinValidator;

public interface LofDetectorParams<T>
	extends OutlierDetectorParams <T>, WithMultiVarParams <T>, HasFastDistanceType <T> {

	@NameCn("相邻点个数")
	@DescCn("构造近邻图使用的相邻点个数")
	ParamInfo <Integer> NUM_NEIGHBORS = ParamInfoFactory
		.createParamInfo("numNeighbors", Integer.class)
		.setDescription("Number of neighbors to construct k-neighbor graph")
		.setHasDefaultValue(5)
		.setValidator(new MinValidator <>(1))
		.build();

	default Integer getNumNeighbors() {
		return get(NUM_NEIGHBORS);
	}

	default T setNumNeighbors(Integer value) {
		return set(NUM_NEIGHBORS, value);
	}
}
