package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.validators.RangeValidator;

/**
 * Params maxFraction.
 */
public interface HasFraction<T> extends WithParams <T> {
	@NameCn("拆分到测试集最大数据比例")
	@DescCn("拆分到测试集最大数据比例")
	ParamInfo <Double> FRACTION = ParamInfoFactory
		.createParamInfo("fraction", Double.class)
		.setDescription("Proportion of data allocated to right output after splitting")
		.setHasDefaultValue(1.0)
		.setValidator(new RangeValidator <>(0.0, 1.0))
		.build();

	default Double getFraction() {
		return get(FRACTION);
	}

	default T setFraction(Double fraction) {
		return set(FRACTION, fraction);
	}

}
