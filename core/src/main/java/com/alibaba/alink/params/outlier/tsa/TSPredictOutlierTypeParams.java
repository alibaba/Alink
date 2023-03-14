package com.alibaba.alink.params.outlier.tsa;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface TSPredictOutlierTypeParams<T> extends WithParams <T> {

	@NameCn("异常检测方法")
	@DescCn("异常检测方法")
	ParamInfo <OutlierType> OUTLIER_TYPE = ParamInfoFactory
		.createParamInfo("outlierType", OutlierType.class)
		.setDescription("time series function type")
		.setHasDefaultValue(OutlierType.SmoothZScore)
		.build();

	default OutlierType getOutlierType() {
		return get(OUTLIER_TYPE);
	}

	default T setOutlierType(OutlierType value) {
		return set(OUTLIER_TYPE, value);
	}

	default T setOutlierType(String value) {
		return set(OUTLIER_TYPE, ParamUtil.searchEnum(OUTLIER_TYPE, value));
	}

	enum OutlierType {
		SmoothZScore,
		ShortMoM
	}
}
