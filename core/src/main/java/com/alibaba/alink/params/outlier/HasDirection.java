package com.alibaba.alink.params.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface HasDirection<T> extends WithParams <T> {

	@NameCn("方向")
	@DescCn("检测异常的方向")
	ParamInfo <Direction> DIRECTION = ParamInfoFactory
		.createParamInfo("direction", Direction.class)
		.setDescription("directionality of the anomalies to be detected")
		.setHasDefaultValue(Direction.BOTH)
		.build();

	default Direction getDirection() {
		return get(DIRECTION);
	}

	default T setDirection(Direction value) {
		return set(DIRECTION, value);
	}

	default T setDirection(String value) {
		return set(DIRECTION,
			ParamUtil.searchEnum(DIRECTION, value));
	}

	enum Direction {
		/**
		 * detect the upper anomalies.
		 */
		POSITIVE,
		/**
		 * detect the lower anomalies.
		 */
		NEGATIVE,
		/**
		 * detect both the upper and the lower anomalies.
		 */
		BOTH
	}
}
