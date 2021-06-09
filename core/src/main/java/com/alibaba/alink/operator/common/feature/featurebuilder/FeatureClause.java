package com.alibaba.alink.operator.common.feature.featurebuilder;

import java.io.Serializable;

public class FeatureClause implements Serializable {
	public String inColName;
	public FeatureClauseOperator op;
	public String outColName;
	//save other params of agg func.
	public Object[] inputParams = new Object[0];
}
