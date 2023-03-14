package com.alibaba.alink.operator.stream.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.similarity.StringSimilarityPairwiseMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.similarity.StringTextPairwiseParams;

/**
 * Calculate the calc between characters in pair.
 */
@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("字符串两两相似度计算")
@NameEn("String Similarity Pairwise")
public final class StringSimilarityPairwiseStreamOp extends MapStreamOp <StringSimilarityPairwiseStreamOp>
	implements StringTextPairwiseParams <StringSimilarityPairwiseStreamOp> {
	private static final long serialVersionUID = 597932987060750046L;

	/**
	 * default constructor
	 */
	public StringSimilarityPairwiseStreamOp() {
		this(null);
	}

	public StringSimilarityPairwiseStreamOp(Params params) {
		super(StringSimilarityPairwiseMapper::new, params);
	}
}
