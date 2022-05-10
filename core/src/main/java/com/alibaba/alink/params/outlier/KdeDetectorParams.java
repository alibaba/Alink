package com.alibaba.alink.params.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.shared.clustering.HasFastDistanceType;
import com.alibaba.alink.params.validators.MinValidator;

public interface KdeDetectorParams<T>
	extends HasKDEKernelType <T>, OutlierDetectorParams <T>, WithMultiVarParams <T>, HasFastDistanceType <T> {

	@NameCn("KDE带宽")
	@DescCn("核密度函数带宽参数")
	ParamInfo <Double> BANDWIDTH = ParamInfoFactory
		.createParamInfo("bandwidth", Double.class)
		.setDescription("bandwidth of kernel dense estimate")
		.setRequired()
		.setValidator(new MinValidator <>(0.))
		.build();

	default double getBandwidth() {
		return get(BANDWIDTH);
	}

	default T setBandwidth(double value) {
		return set(BANDWIDTH, value);
	}

	@NameCn("相邻点个数")
	@DescCn("计算KDE时使用的相邻点个数(默认使用全部点)")
	ParamInfo <Integer> NUM_NEIGHBORS = ParamInfoFactory
		.createParamInfo("numNeighbors", Integer.class)
		.setDescription("Number of neighbors to construct k-neighbor graph")
		.setHasDefaultValue(-1)
		.build();

	default Integer getNumNeighbors() {
		return get(NUM_NEIGHBORS);
	}

	default T setNumNeighbors(Integer value) {
		return set(NUM_NEIGHBORS, value);
	}

}
