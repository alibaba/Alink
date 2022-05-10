package com.alibaba.alink.params.feature.featuregenerator;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.dataproc.HasClause;

public interface SessionTimeWindowParams<T> extends
	GroupTimeWindowParams <T>,
	HasClause <T> {

	@NameCn("会话窗口间隔大小")
	@DescCn("会话窗口间隔大小")
	ParamInfo <String> SESSION_GAP_TIME = ParamInfoFactory
		.createParamInfo("sessionGapTime", String.class)
		.setDescription("session gap time")
		.setRequired()
		.build();

	default String getSessionGapTime() {return get(SESSION_GAP_TIME);}

	default T setSessionGapTime(Double value) {return set(SESSION_GAP_TIME, value.toString());}

	default T setSessionGapTime(Integer value) {
		return set(SESSION_GAP_TIME, value.toString());
	}

	default T setSessionGapTime(String value) {
		return set(SESSION_GAP_TIME, value);
	}

}
