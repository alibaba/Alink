package com.alibaba.alink.operator.stream.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.similarity.StringSimilarityPairwiseMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.similarity.StringTextPairwiseParams;

/**
 * Calculate the calc between characters in pair.
 */
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
