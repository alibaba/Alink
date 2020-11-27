package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.similarity.TextSimilarityPairwiseMapper;
import com.alibaba.alink.params.similarity.StringTextPairwiseParams;

/**
 * Calculate the calc between texts in pair.
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
public final class TextSimilarityPairwiseBatchOp extends MapBatchOp <TextSimilarityPairwiseBatchOp>
	implements StringTextPairwiseParams <TextSimilarityPairwiseBatchOp> {
	private static final long serialVersionUID = 627852765048346223L;

	public TextSimilarityPairwiseBatchOp() {
		this(null);
	}

	public TextSimilarityPairwiseBatchOp(Params params) {
		super(TextSimilarityPairwiseMapper::new, params);
	}
}
