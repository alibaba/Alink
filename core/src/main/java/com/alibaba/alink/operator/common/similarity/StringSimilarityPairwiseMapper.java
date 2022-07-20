package com.alibaba.alink.operator.common.similarity;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.mapper.MISOMapper;
import com.alibaba.alink.operator.common.similarity.similarity.Cosine;
import com.alibaba.alink.operator.common.similarity.similarity.JaccardSimilarity;
import com.alibaba.alink.operator.common.similarity.similarity.LevenshteinSimilarity;
import com.alibaba.alink.operator.common.similarity.similarity.LongestCommonSubsequence;
import com.alibaba.alink.operator.common.similarity.similarity.LongestCommonSubsequenceSimilarity;
import com.alibaba.alink.operator.common.similarity.similarity.SimHashHammingSimilarity;
import com.alibaba.alink.operator.common.similarity.similarity.Similarity;
import com.alibaba.alink.operator.common.similarity.similarity.SubsequenceKernelSimilarity;
import com.alibaba.alink.params.similarity.HasMetric;
import com.alibaba.alink.params.similarity.HasMetric.Metric;
import com.alibaba.alink.params.similarity.HasPaiMetric;
import com.alibaba.alink.params.similarity.StringTextApproxParams;
import com.alibaba.alink.params.similarity.StringTextExactParams;
import com.alibaba.alink.params.similarity.StringTextPairwiseParams;

public class StringSimilarityPairwiseMapper extends MISOMapper {
	private static final long serialVersionUID = 5592024999326980318L;
	private final Similarity similarity;
	private final HasMetric.Metric method;

	public StringSimilarityPairwiseMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.similarity = createSimilarity(this.params);
		this.method = this.params.get(StringTextPairwiseParams.METRIC);
	}

	public static Similarity createSimilarity(Params params) {
		if (params.contains(HasMetric.METRIC) || !params.contains(HasPaiMetric.PAI_METRIC)) {
			params.set(HasPaiMetric.PAI_METRIC, params.get(HasMetric.METRIC).name());
		}
		switch (Metric.valueOf(params.get(HasPaiMetric.PAI_METRIC))) {
			// levenshtein supports metric and calc.
			case LEVENSHTEIN:
			case LEVENSHTEIN_SIM: {
				return new LevenshteinSimilarity();
			}
			// lcs supports metric and calc.
			case LCS:
				return new LongestCommonSubsequence();
			case LCS_SIM: {
				return new LongestCommonSubsequenceSimilarity();
			}
			// ssk supports only calc.
			case SSK: {
				return new SubsequenceKernelSimilarity(params.get(StringTextExactParams.WINDOW_SIZE),
					params.get(StringTextExactParams.LAMBDA));
			}
			// cosine supports only calc.
			case COSINE: {
				return new Cosine(params.get(StringTextExactParams.WINDOW_SIZE));
			}
			// simHash hamming supports metric and calc.
			case SIMHASH_HAMMING:
			case SIMHASH_HAMMING_SIM: {
				return new SimHashHammingSimilarity();
			}
			case JACCARD_SIM: {
				return new JaccardSimilarity(params.get(StringTextApproxParams.SEED),
					params.get(StringTextApproxParams.NUM_HASH_TABLES),
					params.get(StringTextApproxParams.NUM_BUCKET));
			}
			default: {
				throw new AkUnsupportedOperationException("No such calc method");
			}
		}
	}

	public static <T> double calc(T s1, T s2, Metric metric, Similarity similarity) {
		switch (metric) {
			case LEVENSHTEIN:
			case SIMHASH_HAMMING: {
				return (double) similarity.getDistance().calc(s1, s2);
			}
			case LEVENSHTEIN_SIM:
			case LCS_SIM:
			case LCS:
			case COSINE:
			case SSK:
			case SIMHASH_HAMMING_SIM: {
				return similarity.calc(s1, s2);
			}
			case JACCARD_SIM: {
				return JaccardSimilarity.similarity(s1, s2);
			}
			default: {
				throw new AkUnsupportedOperationException("No such calc method " + metric);
			}
		}
	}

	@Override
	protected TypeInformation<?> initOutputColType() {
		return Types.DOUBLE;
	}

	@Override
	protected Object map(Object[] input) {
		if (input.length != 2) {
			throw new AkIllegalDataException("PairWise only supports two input columns!");
		}
		String s1 = (String) input[0];
		String s2 = (String) input[1];
		return calc(s1, s2, method, similarity);
	}
}
