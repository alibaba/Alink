package com.alibaba.alink.common.annotation;

import com.alibaba.alink.common.annotation.ParamCond.CondType;
import com.alibaba.alink.common.annotation.ParamCond.CondValue;
import com.alibaba.alink.common.annotation.ParamCond.CondValueType;
import com.alibaba.alink.common.annotation.ParamMutexRule.ActionType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
@ParamMutexRule(
	name = "vectorCol", type = ActionType.DISABLE,
	cond = @ParamCond(
		name = "featureCols",
		type = CondType.WHEN_VALUES_NOT_IN,
		values = {@CondValue(type = CondValueType.NULL), @CondValue("[]")}
	)
)
@ParamMutexRule(
	name = "featureCols", type = ActionType.DISABLE,
	cond = @ParamCond(
		name = "vectorCol",
		type = CondType.WHEN_VALUES_NOT_IN,
		values = {@CondValue(type = CondValueType.NULL), @CondValue("")}
	)
)
public @interface FeatureColsVectorColMutexRule {
}
