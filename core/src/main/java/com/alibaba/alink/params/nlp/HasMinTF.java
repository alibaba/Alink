package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * Minimum document frequency of a word.
 */
public interface HasMinTF<T> extends WithParams <T> {
	@NameCn("最低词频")
	@DescCn("最低词频，如果词频小于minTF,这个词会被忽略掉。minTF可以是具体的词频也可以是整体词频的比例，如果minTF在[0,1)区间，会被认为是比例。")
	ParamInfo <Double> MIN_TF = ParamInfoFactory
		.createParamInfo("minTF", Double.class)
		.setDescription(
			"When the number word in this document in is below minTF, the word will be ignored. It could be an exact "
				+ "count or a fraction of the document token count. When minTF is within [0, 1), it's used as a "
				+ "fraction.")
		.setHasDefaultValue(1.0)
		.build();

	default double getMinTF() {
		return get(MIN_TF);
	}

	default T setMinTF(Double value) {
		return set(MIN_TF, value);
	}
}
