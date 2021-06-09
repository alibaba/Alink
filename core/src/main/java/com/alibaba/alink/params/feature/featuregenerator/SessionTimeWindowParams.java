package com.alibaba.alink.params.feature.featuregenerator;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.dataproc.HasClause;

public interface SessionTimeWindowParams<T> extends
	GroupTimeWindowParams <T>,
	HasClause <T> {

	//the metric is second.
	ParamInfo <Double> SESSION_GAP_TIME = ParamInfoFactory
		.createParamInfo("sessionGapTime", Double.class)
		.setDescription("session gap time")
		.setRequired()
		.build();

	default Double getSessionGapTime() {return get(SESSION_GAP_TIME);}

	default T setSessionGapTime(Double value) {return set(SESSION_GAP_TIME, value);}

	default T setSessionGapTime(Integer value) {
		return set(SESSION_GAP_TIME, (double) value);
	}
}
