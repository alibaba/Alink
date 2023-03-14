package com.alibaba.alink.params.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasDetectLast<T> extends WithParams <T> {

	@NameCn("只检测最后一行")
	@DescCn("只检测最后一行")
	ParamInfo <Boolean> DETECT_LAST = ParamInfoFactory
		.createParamInfo("detectLast", Boolean.class)
		.setDescription("Only detect the last row.")
		.setHasDefaultValue(false)
		.build();

	default Boolean getDetectLast() {
		return get(DETECT_LAST);
	}

	default T setDetectLast(Boolean val) {
		return set(DETECT_LAST, val);
	}
}
