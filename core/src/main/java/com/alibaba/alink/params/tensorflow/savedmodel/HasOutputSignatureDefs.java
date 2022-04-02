package com.alibaba.alink.params.tensorflow.savedmodel;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasOutputSignatureDefs<T> extends WithParams <T> {

	@NameCn("TF 输出 SignatureDef 名")
	@DescCn("模型的输出 SignatureDef 名，多个输出时用逗号分隔，并且与输出 Schema 一一对应，默认与输出 Schema 中的列名相同")
	ParamInfo <String[]> OUTPUT_SIGNATURE_DEFS = ParamInfoFactory
		.createParamInfo("outputSignatureDefs", String[].class)
		.setDescription("output signature defs")
		.setHasDefaultValue(null)
		.build();

	default String[] getOutputSignatureDefs() {
		return get(OUTPUT_SIGNATURE_DEFS);
	}

	default T setOutputSignatureDefs(String[] value) {
		return set(OUTPUT_SIGNATURE_DEFS, value);
	}
}
