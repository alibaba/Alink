package com.alibaba.alink.operator.common.tree.parallelcart.criteria;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public enum CriteriaType {
	PAI,
	ALINK,
	XGBOOST;

	public static final ParamInfo <CriteriaType> CRITERIA_TYPE = ParamInfoFactory
		.createParamInfo("criteriaType", CriteriaType.class)
		.build();
}
