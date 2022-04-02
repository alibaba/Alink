package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * Params minDF.When the number of documents a word appears in is below minDF, the word will not be included in the
 * dictionary.
 * <p>
 * It could be an exact count or a fraction of the document number count. When minDF is within [0, 1), it's used as a
 * fraction.
 */
public interface HasMinDF<T> extends WithParams <T> {
	@NameCn("最小文档词频")
	@DescCn("如果一个词出现的文档次数小于minDF, 这个词不会被包含在字典中。minTF可以是具体的词频也可以是整体词频的比例，如果minDF在[0,1)区间，会被认为是比例。")
	ParamInfo <Double> MIN_DF = ParamInfoFactory
		.createParamInfo("minDF", Double.class)
		.setDescription(
			"When the number of documents a word appears in is below minDF, the word will not be included in the "
				+ "dictionary. It could be an exact count"
				+ "or a fraction of the document number count. When minDF is within [0, 1), it's used as a fraction.")
		.setHasDefaultValue(1.0)
		.build();

	default double getMinDF() {
		return get(MIN_DF);
	}

	default T setMinDF(Double value) {
		return set(MIN_DF, value);
	}
}
