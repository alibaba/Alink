package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.model.SimpleModelDataConverter;

import java.util.Arrays;
import java.util.HashMap;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

/**
 * @author lqb
 * @ClassName NgramModel
 * @description NgramModel is
 * @date 2019/04/10
 */
public class NGramModelDataConverter
	extends SimpleModelDataConverter <HashMap <String, String>, HashMap <String, String>> {
	public final static String NGRAM = "ngram";
	public final static String TEXT_NGRAM_CNT = "textNgramCnt";
	public final static String TEXT_LENGTH = "textLength";

	public final static String WORD_TYPE = "word";
	public final static String STRING_TYPE = "string";

	final static String NGRAM_CNT = "ngram_cnt";
	final static String TEXT_CNT = "text_cnt";
	final static String MAX_TEXT_LENGTH = "max_text_length";
	final static String MIN_TEXT_LENGTH = "min_text_length";
	final static String AVG_TEXT_LENGTH = "avg_text_length";

	public NGramModelDataConverter() {
	}

	@Override
	public Tuple2 <Params, Iterable <String>> serializeModel(HashMap <String, String> data) {
		return Tuple2.of(new Params(), Arrays.asList(gson.toJson(data)));
	}

	@Override
	public HashMap <String, String> deserializeModel(Params meta, Iterable <String> modelData) {
		return gson.fromJson(modelData.iterator().next(), HashMap.class);
	}
}
