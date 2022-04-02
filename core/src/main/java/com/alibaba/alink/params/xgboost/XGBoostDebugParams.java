package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface XGBoostDebugParams<T> extends WithParams <T> {

	@NameCn("运行模式")
	@DescCn("XGBoost的运行模型，ICQ速度快，但使用内存多，TRIAVIAL速度略慢，但是节省内存，按照流式方式处理。"
		+ "由于训练数据本身在XGBoost运行时已经被缓存进内存，所以存两份和存一份数据的资源消耗和速度对比，还需要进一步的测试。")
	ParamInfo <RunningMode> RUNNING_MODE = ParamInfoFactory
		.createParamInfo("runningMode", RunningMode.class)
		.setHasDefaultValue(RunningMode.TRIVIAL)
		.setDescription("Running mode of xgboost. for debugging")
		.build();

	default RunningMode getRunningMode() {
		return get(RUNNING_MODE);
	}

	default T setRunningMode(RunningMode runningMode) {
		return set(RUNNING_MODE, runningMode);
	}

	default T setRunningMode(String runningMode) {
		return set(RUNNING_MODE, ParamUtil.searchEnum(RUNNING_MODE, runningMode));
	}

	enum RunningMode {
		ICQ,
		TRIVIAL
	}
}
