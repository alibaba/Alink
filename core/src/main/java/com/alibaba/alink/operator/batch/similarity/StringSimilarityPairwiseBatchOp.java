package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.similarity.StringSimilarityPairwiseMapper;
import com.alibaba.alink.params.similarity.StringTextPairwiseParams;

/**
 * Calculate the calc between characters in pair.
 * We support different calc methods, by choosing the parameter "method", you can choose
 * which method you use.
 * LEVENSHTEIN: the minimum number of single-character edits (insertions, deletions or substitutions)
 * required to change one word into the other.
 * LEVENSHTEIN_SIM: calc = 1.0 - Normalized Distance.
 * LCS: the longest subsequence common to the two inputs.
 * LCS_SIM: Similarity = Distance / max(Left Length, Right Length)
 * COSINE: a measure of calc between two non-zero vectors of an inner product
 * space that measures the cosine of the angle between them.
 * SSK: maps strings to a feature vector indexed by all k tuples of characters, and
 * get the dot product.
 * SIMHASH_HAMMING: Hash the inputs to BIT_LENGTH size, and calculate the hamming distance.
 * SIMHASH_HAMMING_SIM: Similarity = 1.0 - distance / BIT_LENGTH.
 * MINHASH_SIM: MinHashSim = P(hmin(A) = hmin(B)) = Count(I(hmin(A) = hmin(B))) / k.
 * JACCARD_SIM: JaccardSim = |A ∩ B| / |A ∪ B| = |A ∩ B| / (|A| + |B| - |A ∩ B|)
 */
@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("字符串两两相似度计算")
@NameEn("String Similarity Pairwise")
public class StringSimilarityPairwiseBatchOp extends MapBatchOp <StringSimilarityPairwiseBatchOp>
	implements StringTextPairwiseParams <StringSimilarityPairwiseBatchOp> {
	private static final long serialVersionUID = 6952374807123805800L;

	/**
	 * default constructor.
	 */
	public StringSimilarityPairwiseBatchOp() {
		this(null);
	}

	public StringSimilarityPairwiseBatchOp(Params params) {
		super(StringSimilarityPairwiseMapper::new, params);
	}

}
