package com.alibaba.alink.params.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface DbscanLocalParams<T>
	extends DbscanTrainParams <T>, DbscanPredictParams <T> {

	@NameCn("是否使用近似算法")
	@DescCn("是否使用基于近似最近邻算法实现的DBSCAN算法")
	ParamInfo <Boolean> USE_APPROX_ALGO = ParamInfoFactory
		.createParamInfo("useApproxAlgo", Boolean.class)
		.setDescription("use VectorApproxNearestNeighbor algo, default is false")
		.setHasDefaultValue(false)
		.build();

	default Boolean getUseApproxAlgo() {return get(USE_APPROX_ALGO);}

	default T setUseApproxAlgo(Boolean value) {return set(USE_APPROX_ALGO, value);}
}
