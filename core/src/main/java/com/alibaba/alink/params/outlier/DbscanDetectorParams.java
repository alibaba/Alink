package com.alibaba.alink.params.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.shared.clustering.HasFastDistanceType;
import com.alibaba.alink.params.validators.MinValidator;

/**
 * 算法相关: 需要设置聚类距离限制(epsilon)、规模限制(minPoints)、距离类型(distanceType) 数据转换相关: 需要指定列名（featureCols）和MTable名（selectedCol）
 * 使用相关：epsilon不确定时可以设为-1，将自动选择合适的值
 *
 * @param <T>
 */
public interface DbscanDetectorParams<T> extends WithMultiVarParams <T>,
	HasFastDistanceType <T> {
	ParamInfo <Integer> MIN_POINTS = ParamInfoFactory
		.createParamInfo("minPoints", Integer.class)
		.setDescription("The number of samples in a neighborhood smaller than this value are outliers")
		.setHasDefaultValue(3)
		.setValidator(new MinValidator <>(0))
		.build();
	ParamInfo <Double> EPSILON = ParamInfoFactory
		.createParamInfo("epsilon", Double.class)
		.setDescription("The maximum distance between two samples")
		.setValidator(new MinValidator <>(0.0))
		.build();
	default Integer getMinPoints() {
		return get(MIN_POINTS);
	}
	default T setMinPoints(Integer value) {
		return set(MIN_POINTS, value);
	}
	default Double getEpsilon() {
		return get(EPSILON);
	}
	default T setEpsilon(Double value) {
		return set(EPSILON, value);
	}
}