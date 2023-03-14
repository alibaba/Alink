package com.alibaba.alink.params.clustering.lda;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * doc parameter array.
 */
public interface HasAlphaArray<T> extends WithParams <T> {
	@NameCn("alpha数组")
	@DescCn("alpha数组")
	ParamInfo <double[]> ALPHA_ARRAY = ParamInfoFactory
		.createParamInfo("alphaArray", double[].class)
		.setDescription(
			"alpha.Concentration parameter (commonly named \"alpha\") for the prior placed on documents' distributions"
				+ " over topics (\"beta\").")
		.setHasDefaultValue(null)
		.build();

	default double[] getAlphaArray() {
		return get(ALPHA_ARRAY);
	}

	default T setAlphaArray(double[] value) {
		return set(ALPHA_ARRAY, value);
	}
}
