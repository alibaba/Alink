package com.alibaba.alink.pipeline.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.similarity.TextSimilarityPairwiseMapper;
import com.alibaba.alink.params.similarity.StringTextPairwiseParams;
import com.alibaba.alink.pipeline.MapTransformer;

@NameCn("文本两两相似度计算")
public class TextSimilarityPairwise extends MapTransformer <TextSimilarityPairwise> implements
	StringTextPairwiseParams <TextSimilarityPairwise> {

	public TextSimilarityPairwise() {
		this(null);
	}

	public TextSimilarityPairwise(Params params) {
		super(TextSimilarityPairwiseMapper::new, params);
	}
}
