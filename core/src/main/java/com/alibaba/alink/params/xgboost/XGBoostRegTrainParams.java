package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.io.shared.HasPluginVersion;

public interface XGBoostRegTrainParams<T> extends
	XGBoostCommandLineParams <T>,
	XGBoostInputParams <T>,
	XGBoostTreeBoosterParams <T>,
	HasTweedieVariancePower <T>,
	HasBaseScore <T>,
	HasPluginVersion <T>,
	XGBoostDebugParams<T> {

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
		REG_GAMMA,
		REG_TWEEDIE
	}
}
