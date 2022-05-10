package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.plugin.PluginUtils;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.io.shared.HasPluginVersion;

public interface XGBoostTrainParams<T> extends
	XGBoostCommandLineParams <T>,
	XGBoostInputParams <T>,
	XGBoostTreeBoosterParams <T>,
	HasBaseScore <T>,
	HasXGBoostPluginVersion<T>,
	XGBoostDebugParams<T> {

	@NameCn("objective")
	@DescCn("objective")
	ParamInfo <Objective> OBJECTIVE = ParamInfoFactory
		.createParamInfo("objective", Objective.class)
		.setDescription("Specify the learning task and the corresponding learning objective.")
		.setHasDefaultValue(Objective.BINARY_LOGISTIC)
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
		BINARY_LOGISTIC,
		BINARY_LOGITRAW,
		BINARY_HINGE,
		MULTI_SOFTMAX,
		MULTI_SOFTPROB
	}
}
