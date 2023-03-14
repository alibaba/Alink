package com.alibaba.alink.params.outlier.tsa;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

public interface HasBoxPlotRoundMode<T> extends WithParams <T> {
	ParamInfo <RoundMode> ROUND_MODE = ParamInfoFactory
		.createParamInfo("roundMode", RoundMode.class)
		.setDescription("The strategy to choose quartile value if its index is not an integer. " +
			"<ul>" +
			"<li>ceil: ⌈index⌉</li>" +
			"<li>floor: ⌊index⌋</li>" +
			"<li>average: choose the average of the ceil index value and the floor index value</li>" +
			"</ul>" + "<p>")
		.setHasDefaultValue(RoundMode.CEIL)
		.build();

	default RoundMode getRoundMode() {
		return get(ROUND_MODE);
	}

	default T setRoundMode(String value) {
		return set(ROUND_MODE, ParamUtil.searchEnum(ROUND_MODE, value));
	}

	default T setRoundMode(RoundMode value) {
		return set(ROUND_MODE, value);
	}

	enum RoundMode {
		/**
		 * ⌊index⌋
		 */
		FLOOR,
		/**
		 * ⌈index⌉
		 */
		CEIL,
		/**
		 * choose the average of the ceil index value and the floor index value
		 */
		AVERAGE
	}
}
