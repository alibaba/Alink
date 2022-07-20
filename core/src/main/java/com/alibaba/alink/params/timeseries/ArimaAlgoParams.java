package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.timeseries.holtwinters.HasSeasonalPeriod;
import com.alibaba.alink.params.validators.ArrayLengthValidator;

public interface ArimaAlgoParams<T> extends
	HasEstmateMethod <T>,
	HasSeasonalPeriod <T> {

	@NameCn("模型(p, d, q)")
	@DescCn("模型(p, d, q)")
	ParamInfo <Integer[]> ORDER = ParamInfoFactory
		.createParamInfo("order", Integer[].class)
		.setDescription("p,d,q")
		.setValidator(new ArrayLengthValidator(3))
		.setRequired()
		.build();

	default Integer[] getOrder() {
		return get(ORDER);
	}

	default T setOrder(Integer... value) {
		return set(ORDER, value);
	}

	@NameCn("季节模型(p, d, q)")
	@DescCn("季节模型(p, d, q)")
	ParamInfo <int[]> SEASONAL_ORDER = ParamInfoFactory
		.createParamInfo("seasonalOrder", int[].class)
		.setDescription("p,d,q")
		.setOptional()
		.setHasDefaultValue(null)
		.build();

	default int[] getSeasonalOrder() {
		return get(SEASONAL_ORDER);
	}

	default T setSeasonalOrder(int[] value) {
		return set(SEASONAL_ORDER, value);
	}

}
