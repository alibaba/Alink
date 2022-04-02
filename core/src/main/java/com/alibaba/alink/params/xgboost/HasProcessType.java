package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface HasProcessType<T> extends WithParams <T> {

	@NameCn("ProcessType")
	@DescCn("ProcessType")
	ParamInfo <ProcessType> PROCESS_TYPE = ParamInfoFactory
		.createParamInfo("processType", ProcessType.class)
		.setDescription("A type of boosting process to run.")
		.setHasDefaultValue(ProcessType.DEFAULT)
		.build();

	default ProcessType getProcessType() {
		return get(PROCESS_TYPE);
	}

	default T setProcessType(ProcessType processType) {
		return set(PROCESS_TYPE, processType);
	}

	default T setProcessType(String processType) {
		return set(PROCESS_TYPE, ParamUtil.searchEnum(PROCESS_TYPE, processType));
	}

	enum ProcessType {
		DEFAULT,
		UPDATE
	}
}
