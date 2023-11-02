package com.alibaba.alink.params.classification;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.shared.HasSmoothing;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorCol;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;

/**
 * Parameters of text naive bayes training process.
 */
public interface FlinkMLModelParams<T> extends
	WithParams<T> {

	@NameCn("类")
	@DescCn("类路径")
	ParamInfo <String> CLASS_NAME = ParamInfoFactory
		.createParamInfo("classPath", String.class)
		.setDescription("Class path.")
		.setRequired()
		.build();

	default String getClassName() {
		return get(CLASS_NAME);
	}

	default T setClassName(String value) {
		return set(CLASS_NAME, value);
	}
}
