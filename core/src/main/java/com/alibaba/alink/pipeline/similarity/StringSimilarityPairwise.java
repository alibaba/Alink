package com.alibaba.alink.pipeline.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.similarity.StringSimilarityPairwiseMapper;
import com.alibaba.alink.params.similarity.StringTextPairwiseParams;
import com.alibaba.alink.pipeline.MapTransformer;

@NameCn("字符串两两相似度计算")
public class StringSimilarityPairwise extends MapTransformer <StringSimilarityPairwise> implements
	StringTextPairwiseParams <StringSimilarityPairwise> {

	public StringSimilarityPairwise() {
		this(null);
	}

	public StringSimilarityPairwise(Params params) {
		super(StringSimilarityPairwiseMapper::new, params);
	}
}
