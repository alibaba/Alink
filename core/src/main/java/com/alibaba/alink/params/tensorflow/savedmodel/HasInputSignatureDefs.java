package com.alibaba.alink.params.tensorflow.savedmodel;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasInputSignatureDefs<T> extends WithParams <T> {
	@NameCn("输入 SignatureDef")
	@DescCn("SavedModel 模型的输入 SignatureDef 名，用逗号分隔，需要与输入列一一对应，默认与选择列相同")
	ParamInfo <String[]> INPUT_SIGNATURE_DEFS = ParamInfoFactory
		.createParamInfo("inputSignatureDefs", String[].class)
		.setDescription("input signature defs corresponding to selected columns in the SavedModel")
		.setHasDefaultValue(null)
		.build();

	default String[] getInputSignatureDefs() {
		return get(INPUT_SIGNATURE_DEFS);
	}

	default T setInputSignatureDefs(String[] value) {
		return set(INPUT_SIGNATURE_DEFS, value);
	}
}
