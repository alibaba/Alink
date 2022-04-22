package com.alibaba.alink.common.annotation;

import java.util.HashMap;
import java.util.Map;

/**
 * Specify a parameter condition.
 * <p>
 * NOTE: `null` can be treated as a special value when checking the condition is satisfied, but cannot be used in
 * `values()`.
 */
public @interface ParamCond {
	String name();

	CondType type();

	/**
	 * Parameter values in string representation.
	 * <p>
	 * Only used when {@link ParamCond#type()} is {@link CondType#WHEN_IN_VALUES} and {@link
	 * CondType#WHEN_NOT_IN_VALUES}.
	 * <p>
	 * 'null' string cannot be used to represent null value.
	 */
	String[] values() default {};

	enum CondType {
		WHEN_NULL,    // not set or set to null
		WHEN_NOT_NULL,    // set to non-null values
		WHEN_IN_VALUES,    // set to one value in `values`
		WHEN_NOT_IN_VALUES;    // set to a value not in `value` or not set

		static final Map <CondType, CondType> INVERTED_COND_MAP = new HashMap <>();

		static {
			INVERTED_COND_MAP.put(WHEN_IN_VALUES, WHEN_NOT_IN_VALUES);
			INVERTED_COND_MAP.put(WHEN_NOT_IN_VALUES, WHEN_IN_VALUES);
			INVERTED_COND_MAP.put(WHEN_NULL, WHEN_NOT_NULL);
			INVERTED_COND_MAP.put(WHEN_NOT_NULL, WHEN_NULL);
		}

		public CondType invert() {
			return INVERTED_COND_MAP.get(this);
		}
	}
}
