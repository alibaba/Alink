package com.alibaba.alink.params.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasOutlierThreshold<T> extends WithParams <T> {
	@NameCn("异常评分阈值")
	@DescCn("只有评分大于该阈值才会被认为是异常点")
	ParamInfo <Double> OUTLIER_THRESHOLD = ParamInfoFactory
		.createParamInfo("outlierThreshold", Double.class)
		.setDescription("threshold for outlier detection")
		.build();

	default Double getOutlierThreshold() {return get(OUTLIER_THRESHOLD);}

	default T setOutlierThreshold(Double value) {return set(OUTLIER_THRESHOLD, value);}
}
