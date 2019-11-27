package com.alibaba.alink.params.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;
import com.alibaba.alink.params.shared.clustering.HasEpsilonDv00001;
import com.alibaba.alink.params.shared.clustering.HasKDefaultAs2;

/**
 * Common params for KMeans.
 */
public interface BaseKMeansTrainParams<T> extends WithParams<T>,
	HasKDefaultAs2<T>,
	HasEpsilonDv00001 <T> {

	ParamInfo <Integer> MAX_ITER = ParamInfoFactory
		.createParamInfo("maxIter", Integer.class)
		.setDescription("Maximum iterations, the default value is 20")
		.setHasDefaultValue(20)
		.setAlias(new String[] {"numIter"})
		.build();
	ParamInfo <String> INIT_MODE = ParamInfoFactory
		.createParamInfo("initMode", String.class)
		.setDescription("Methods to get initial centers, support K_MEANS_PARALLEL and RANDOM!")
		.setHasDefaultValue("K_MEANS_PARALLEL")
		.build();
	ParamInfo <Integer> INIT_STEPS = ParamInfoFactory
		.createParamInfo("initSteps", Integer.class)
		.setDescription("When initMode is K_MEANS_PARALLEL, it defines the steps of iteration. The default value is 2.")
		.setHasDefaultValue(2)
		.build();

	default Integer getMaxIter() {return get(MAX_ITER);}

	default T setMaxIter(Integer value) {return set(MAX_ITER, value);}

	default String getInitMode() {
		return get(INIT_MODE);
	}

	default T setInitMode(String value) {
		return set(INIT_MODE, value);
	}

	default Integer getInitSteps() {
		return get(INIT_STEPS);
	}

	default T setInitSteps(Integer value) {
		return set(INIT_STEPS, value);
	}
}
