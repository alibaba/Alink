package com.alibaba.alink.params.tuning;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface BayesTuningParams<T> extends WithParams <T> {
	@NameCn("超参数优化方法")
	@DescCn("指定用来找到最优的超参数组合的算法名称, 可取tpe(Tree-Structured Parzen Estimator)和gp(Gaussian Process).")
	ParamInfo <BayesStrategy> BAYES_STRATEGY = ParamInfoFactory
		.createParamInfo("bayesStrategy", BayesStrategy.class)
		.setDescription("the strategy of bayes optim")
		.setHasDefaultValue(BayesStrategy.GP)
		.build();

	default BayesStrategy getBayesStrategy() {
		return get(BAYES_STRATEGY);
	}

	default T setBayesStrategy(BayesStrategy value) {
		return set(BAYES_STRATEGY, value);
	}

	default T setBayesStrategy(String value) {
		return set(BAYES_STRATEGY, ParamUtil.searchEnum(BAYES_STRATEGY, value));
	}

	enum BayesStrategy {
		GP, // Gaussian Process
		TPE, //Tree-Structured Parzen Estimator
	}

	@NameCn("预热轮数")
	@DescCn("贝叶斯超参数优化需要用随机生成的参数预热.")
	ParamInfo <Integer> BAYES_NUM_STARTUP = ParamInfoFactory
		.createParamInfo("bayesNumStartup", Integer.class)
		.setDescription("The first N hyperparameters are generated fully randomly for warming up.")
		.setHasDefaultValue(20)
		.build();

	default T setBayesNumStartup(int value) {
		return set(BAYES_NUM_STARTUP, value);
	}

	default int getBayesNumStartup() {
		return get(BAYES_NUM_STARTUP);
	}

	@NameCn("候选参数数量")
	@DescCn("贝叶斯超参数优化每轮从N个候选参数估计最好的")
	ParamInfo <Integer> BAYES_NUM_CANDIDATES = ParamInfoFactory
		.createParamInfo("bayesNumCandidates", Integer.class)
		.setDescription("In each iteration, for each hyperparameter samples N candidates and choose the best one.")
		.setHasDefaultValue(20)
		.build();

	default T setBayesNumCandidates(int value) {
		return set(BAYES_NUM_CANDIDATES, value);
	}

	default int getBayesNumCandidates() {
		return get(BAYES_NUM_CANDIDATES);
	}

	@NameCn("历史数据权重下降率")
	@DescCn("TPE算法将降低最近N条之前的历史数据在计算时的权重")
	ParamInfo <Integer> BAYES_TPE_LINEAR_FORGETTING = ParamInfoFactory
		.createParamInfo("bayesTpeLinearForgetting", Integer.class)
		.setDescription("This is a param for TPE algorithm. TPE will lower the weights of old trials.")
		.setHasDefaultValue(21)
		.build();

	default T setBayesTpeLinearForgetting(int value) {
		return set(BAYES_TPE_LINEAR_FORGETTING, value);
	}

	default int getBayesTpeLinearForgetting() {
		return get(BAYES_TPE_LINEAR_FORGETTING);
	}
}
