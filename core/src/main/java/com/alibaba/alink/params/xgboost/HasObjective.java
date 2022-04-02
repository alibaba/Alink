package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface HasObjective<T> extends WithParams <T> {

	@NameCn("objective")
	@DescCn("objective")
	ParamInfo <Objective> OBJECTIVE = ParamInfoFactory
		.createParamInfo("objective", Objective.class)
		.setDescription("Specify the learning task and the corresponding learning objective.")
		.setHasDefaultValue(Objective.REG_SQUAREDERROR)
		.build();

	default Objective getObjective() {
		return get(OBJECTIVE);
	}

	default T setObjective(Objective objective) {
		return set(OBJECTIVE, objective);
	}

	default T setObjective(String objective) {
		return set(OBJECTIVE, ParamUtil.searchEnum(OBJECTIVE, objective));
	}

	enum Objective {
		REG_SQUAREDERROR,
		REG_SQUAREDLOGERROR,
		REG_LOGISTIC,
		REG_PSEUDOHUBERERROR,
		BINARY_LOGISTIC,
		BINARY_LOGITRAW,
		BINARY_HINGE,
		COUNT_POISSON,
		SURVIVAL_COX,
		SURVIVAL_AFT,
		SURVIVAL_AFT_LOSS_DISTRIBUTION,
		MULTI_SOFTMAX,
		MULTI_SOFTPROB,
		RANK_PAIRWISE,
		RANK_NDCG,
		RANK_MAP,
		REG_GAMMA,
		REG_TWEEDIE
	}
}
