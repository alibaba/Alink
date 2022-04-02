package com.alibaba.alink.common.annotation;

import java.util.HashMap;
import java.util.Map;

public @interface ParamCond {
	String name();

	CondType type();

	CondValue[] values() default {};

	enum CondType {
		WHEN_VALUES_IN,
		WHEN_VALUES_NOT_IN;

		static final Map <CondType, CondType> INVERTED_COND_MAP = new HashMap <>();

		static {
			INVERTED_COND_MAP.put(WHEN_VALUES_IN, WHEN_VALUES_NOT_IN);
			INVERTED_COND_MAP.put(WHEN_VALUES_NOT_IN, WHEN_VALUES_IN);
		}

		CondType invert() {
			return INVERTED_COND_MAP.get(this);
		}
	}

	enum CondValueType {
		NULL,
		STRING
	}

	@interface CondValue {
		CondValueType type() default CondValueType.STRING;
		String value() default "";
	}
}
