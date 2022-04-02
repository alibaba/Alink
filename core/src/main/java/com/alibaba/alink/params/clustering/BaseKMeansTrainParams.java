package com.alibaba.alink.params.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.clustering.kmeans.InitMode;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.shared.clustering.HasEpsilonDefaultAs00001;
import com.alibaba.alink.params.shared.clustering.HasKDefaultAs2;

/**
 * Common params for KMeans.
 */
public interface BaseKMeansTrainParams<T> extends WithParams <T>,
	HasKDefaultAs2 <T>,
	HasEpsilonDefaultAs00001 <T>,
	HasRandomSeed <T> {

	@NameCn("最大迭代步数")
	@DescCn("最大迭代步数，默认为 50。")
	ParamInfo <Integer> MAX_ITER = ParamInfoFactory
		.createParamInfo("maxIter", Integer.class)
		.setDescription("Maximum iterations, the default value is 20")
		.setHasDefaultValue(50)
		.setAlias(new String[] {"numIter"})
		.build();
	@NameCn("中心点初始化方法")
	@DescCn("初始化中心点的方法，支持\"K_MEANS_PARALLEL\"和\"RANDOM\"")
	ParamInfo <InitMode> INIT_MODE = ParamInfoFactory
		.createParamInfo("initMode", InitMode.class)
		.setDescription("Methods to get initial centers, support K_MEANS_PARALLEL and RANDOM!")
		.setHasDefaultValue(InitMode.RANDOM)
		.build();
	@NameCn("k-means++初始化迭代步数")
	@DescCn("k-means初始化中心点时迭代的步数")
	ParamInfo <Integer> INIT_STEPS = ParamInfoFactory
		.createParamInfo("initSteps", Integer.class)
		.setDescription("When initMode is K_MEANS_PARALLEL, it defines the steps of iteration. The default value is "
			+ "2.")
		.setHasDefaultValue(2)
		.build();

	default Integer getMaxIter() {return get(MAX_ITER);}

	default T setMaxIter(Integer value) {return set(MAX_ITER, value);}

	default InitMode getInitMode() {
		return get(INIT_MODE);
	}

	default T setInitMode(InitMode value) {
		return set(INIT_MODE, value);
	}

	default T setInitMode(String value) {
		return set(INIT_MODE, ParamUtil.searchEnum(INIT_MODE, value));
	}

	default Integer getInitSteps() {
		return get(INIT_STEPS);
	}

	default T setInitSteps(Integer value) {
		return set(INIT_STEPS, value);
	}
}
