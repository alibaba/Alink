package com.alibaba.alink.common.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.HashMap;
import java.util.Map;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
@Repeatable(ParamMutexRules.class)
public @interface ParamMutexRule {
	String name();

	ActionType type();

	ParamCond cond();
	
	boolean triggerWhenInverted() default true;

	enum ActionType {
		ENABLE,
		DISABLE,
		SHOW,
		HIDE;

		static final Map <ActionType, ActionType> INVERTED_ACTION_MAP = new HashMap <>();

		static {
			INVERTED_ACTION_MAP.put(ENABLE, DISABLE);
			INVERTED_ACTION_MAP.put(DISABLE, ENABLE);
			INVERTED_ACTION_MAP.put(SHOW, HIDE);
			INVERTED_ACTION_MAP.put(HIDE, SHOW);
		}

		ActionType invert() {
			return INVERTED_ACTION_MAP.get(this);
		}
	}
}
