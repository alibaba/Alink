package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.ParamUtil;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasStringOrderTypeDefaultAsRandom<T> extends WithParams<T> {
	ParamInfo<StringOrderType> STRING_ORDER_TYPE = ParamInfoFactory
		.createParamInfo("stringOrderType", StringOrderType.class)
		.setDescription(
			"String order type, one of \"random\", \"frequency_asc\", \"frequency_desc\", \"alphabet_asc\", "
				+ "\"alphabet_desc\".")
		.setHasDefaultValue(StringOrderType.RANDOM)
		.build();

	default StringOrderType getStringOrderType() {
		return get(STRING_ORDER_TYPE);
	}

	default T setStringOrderType(StringOrderType value) {
		return set(STRING_ORDER_TYPE, value);
	}

	default T setStringOrderType(String value) {
		return set(STRING_ORDER_TYPE, ParamUtil.searchEnum(STRING_ORDER_TYPE, value));
	}

	/**
	 * Specifies how to order tokens.
	 */
	enum StringOrderType {
		/**
		 * Index randomly.
		 */
		RANDOM,
		/**
		 * Ordered by token frequency in ascending order.
		 */
		FREQUENCY_ASC,
		/**
		 * Ordered by token frequency in descending order.
		 */
		FREQUENCY_DESC,
		/**
		 * Ordered by ascending alphabet order ascending.
		 */
		ALPHABET_ASC,
		/**
		 * Ordered by descending alphabet order ascending.
		 */
		ALPHABET_DESC
	}
}
