package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.nlp.bert.BertTokenizerMapper;
import com.alibaba.alink.pipeline.MapTransformer;

public class BertTokenizer extends MapTransformer <BertTokenizer> {

	public BertTokenizer() {
		this(new Params());
	}

	public BertTokenizer(Params params) {
		super(BertTokenizerMapper::new, params);
	}
}
