package com.alibaba.alink.operator.common.similarity;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.params.shared.clustering.HasKDefaultAs2;
import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import com.alibaba.alink.params.similarity.HasMetric;
import com.alibaba.alink.params.similarity.StringTextPairwiseParams;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertArrayEquals;

public class StringSimilarityPairwiseMapperTest {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static TableSchema dataSchema = new TableSchema(new String[] {"col0", "col1"}, new TypeInformation[] {
		Types.STRING, Types.STRING});

	@Test
	public void testSimilarity() {
		Params params = new Params()
			.set(HasSelectedCols.SELECTED_COLS, dataSchema.getFieldNames())
			.set(HasOutputCol.OUTPUT_COL, "res");
		StringSimilarityPairwiseMapper mapper;
		Row[] array =
			new Row[] {
				Row.of("北京", "北京"),
				Row.of("北京欢迎", "中国人民"),
				Row.of("Beijing", "Beijing"),
				Row.of("Beijing", "Chinese"),
				Row.of("Good Morning!", "Good Evening!")
			};
		Double[] res = new Double[array.length];

		// LEVENSHTEIN
		params.set(StringTextPairwiseParams.METRIC, HasMetric.Metric.LEVENSHTEIN);
		Double[] levenshtein = new Double[] {0.0, 4.0, 0.0, 6.0, 3.0};
		mapper = new StringSimilarityPairwiseMapper(dataSchema, params);
		for (int i = 0; i < array.length; i++) {
			res[i] = (double) mapper.map(new Object[] {array[i].getField(0), array[i].getField(1)});
		}
		assertArrayEquals(levenshtein, res);

		// LEVENSHTEIN_SIM
		Double[] levenshteinSim = new Double[] {1.0, 0.0, 1.0, 0.1428571428571429, 0.7692307692307692};
		params.set(StringTextPairwiseParams.METRIC, HasMetric.Metric.LEVENSHTEIN_SIM);
		mapper = new StringSimilarityPairwiseMapper(dataSchema, params);
		for (int i = 0; i < array.length; i++) {
			res[i] = (double) mapper.map(new Object[] {array[i].getField(0), array[i].getField(1)});
		}
		assertArrayEquals(levenshteinSim, res);

		// LCS
		Double[] lcs = new Double[] {2.0, 0.0, 7.0, 2.0, 10.0};
		params.set(StringTextPairwiseParams.METRIC, HasMetric.Metric.LCS);
		mapper = new StringSimilarityPairwiseMapper(dataSchema, params);
		for (int i = 0; i < array.length; i++) {
			res[i] = (double) mapper.map(new Object[] {array[i].getField(0), array[i].getField(1)});
		}
		assertArrayEquals(lcs, res);

		// LCS_SIM
		Double[] lcsSim = new Double[] {1.0, 0.0, 1.0, 0.2857142857142857, 0.7692307692307693};
		params.set(StringTextPairwiseParams.METRIC, HasMetric.Metric.LCS_SIM);
		mapper = new StringSimilarityPairwiseMapper(dataSchema, params);
		for (int i = 0; i < array.length; i++) {
			res[i] = (double) mapper.map(new Object[] {array[i].getField(0), array[i].getField(1)});
		}
		assertArrayEquals(lcsSim, res);

		// SSK
		Double[] ssk = new Double[] {1.0, 0.0, 1.0, 0.14692654669369054, 0.6636536344498839};
		params.set(StringTextPairwiseParams.METRIC, HasMetric.Metric.SSK);
		mapper = new StringSimilarityPairwiseMapper(dataSchema, params);
		for (int i = 0; i < array.length; i++) {
			res[i] = (double) mapper.map(new Object[] {array[i].getField(0), array[i].getField(1)});
		}
		assertArrayEquals(ssk, res);

		// COSINE
		Double[] cosine = new Double[] {1.0, 0.0, 1.0, 0.0, 0.5454545454545454};
		params.set(StringTextPairwiseParams.METRIC, HasMetric.Metric.COSINE).set(HasKDefaultAs2.K, 3);
		mapper = new StringSimilarityPairwiseMapper(dataSchema, params);
		for (int i = 0; i < array.length; i++) {
			res[i] = (double) mapper.map(new Object[] {array[i].getField(0), array[i].getField(1)});
		}
		assertArrayEquals(cosine, res);

		// SIMHASH_HAMMING
		Double[] simHash = new Double[] {0.0, 15.0, 0.0, 10.0, 6.0};
		params.set(StringTextPairwiseParams.METRIC, HasMetric.Metric.SIMHASH_HAMMING);
		mapper = new StringSimilarityPairwiseMapper(dataSchema, params);
		for (int i = 0; i < array.length; i++) {
			res[i] = (double) mapper.map(new Object[] {array[i].getField(0), array[i].getField(1)});
		}
		assertArrayEquals(simHash, res);

		// SIMHASH_HAMMING_SIM
		Double[] simHashSim = new Double[] {1.0, 0.765625, 1.0, 0.84375, 0.90625};
		params.set(StringTextPairwiseParams.METRIC, HasMetric.Metric.SIMHASH_HAMMING_SIM);
		mapper = new StringSimilarityPairwiseMapper(dataSchema, params);
		for (int i = 0; i < array.length; i++) {
			res[i] = (double) mapper.map(new Object[] {array[i].getField(0), array[i].getField(1)});
		}
		assertArrayEquals(simHashSim, res);

		// MINHASH_SIM
		res = new Double[3];
		Double[] minHashSim = new Double[] {1.0, 0.0, 1.0};
		params.set(StringTextPairwiseParams.METRIC, HasMetric.Metric.JACCARD_SIM);
		mapper = new StringSimilarityPairwiseMapper(dataSchema, params);
		for (int i = 0; i < res.length; i++) {
			res[i] = (double) mapper.map(new Object[] {array[i].getField(0), array[i].getField(1)});
		}
		assertArrayEquals(minHashSim, res);

		thrown.expect(RuntimeException.class);
		mapper.map(new Object[] {"s1", "s2", "s3"});
	}
}