package com.alibaba.alink.params.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMaxOutlierRatio<T> extends WithParams <T> {

	@NameCn("最大异常点比例")
	@DescCn("算法检测异常点的最大比例")
	ParamInfo <Double> MAX_OUTLIER_RATIO = ParamInfoFactory
		.createParamInfo("maxOutlierRatio", Double.class)
		.setDescription("the max ratio of outlier that" +
			" algo will detect as a percentage pf the data.")
		.build();

	default Double getMaxOutlierRatio() {
		return get(MAX_OUTLIER_RATIO);
	}

	default T setMaxOutlierRatio(Double value) {
		return set(MAX_OUTLIER_RATIO, value);
	}
}
