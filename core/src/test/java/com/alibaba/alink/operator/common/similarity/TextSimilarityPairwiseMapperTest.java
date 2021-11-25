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
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertArrayEquals;

/**
 * Test for TextSimilarityPairwiseMapper.
 */

public class TextSimilarityPairwiseMapperTest extends AlinkTestBase {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static TableSchema dataSchema = new TableSchema(new String[] {"col0", "col1"}, new TypeInformation[] {
		Types.STRING, Types.STRING});

	@Test
	public void testTextSimilarity() {
		Params params = new Params()
			.set(HasSelectedCols.SELECTED_COLS, dataSchema.getFieldNames())
			.set(HasOutputCol.OUTPUT_COL, "res");
		TextSimilarityPairwiseMapper mapper;

		Row[] array =
			new Row[] {
				Row.of("北京", "北京"),
				Row.of("北京 欢迎", "中国 人民"),
				Row.of("北京 欢迎", "中国 北京"),
				Row.of("Good Morning!", "Good Evening!")
			};

		Double[] res = new Double[array.length];

		// LEVENSHTEIN
		Double[] levenshtein = new Double[] {0.0, 2.0, 2.0, 1.0};
		params.set(StringTextPairwiseParams.METRIC, HasMetric.Metric.LEVENSHTEIN);
		mapper = new TextSimilarityPairwiseMapper(dataSchema, params);
		for (int i = 0; i < array.length; i++) {
			res[i] = (double) mapper.map(new Object[] {array[i].getField(0), array[i].getField(1)});
		}
		assertArrayEquals(levenshtein, res);

		// LEVENSHTEIN_SIM
		Double[] levenshteinSim = new Double[] {1.0, 0.0, 0.0, 0.5};
		params.set(StringTextPairwiseParams.METRIC, HasMetric.Metric.LEVENSHTEIN_SIM);
		mapper = new TextSimilarityPairwiseMapper(dataSchema, params);
		for (int i = 0; i < array.length; i++) {
			res[i] = (double) mapper.map(new Object[] {array[i].getField(0), array[i].getField(1)});
		}
		assertArrayEquals(levenshteinSim, res);

		// LCS
		Double[] lcs = new Double[] {1.0, 0.0, 1.0, 1.0};
		params.set(StringTextPairwiseParams.METRIC, HasMetric.Metric.LCS);
		mapper = new TextSimilarityPairwiseMapper(dataSchema, params);
		for (int i = 0; i < array.length; i++) {
			res[i] = (double) mapper.map(new Object[] {array[i].getField(0), array[i].getField(1)});
		}
		assertArrayEquals(lcs, res);

		// LCS_SIM
		Double[] lcsSim = new Double[] {1.0, 0.0, 0.5, 0.5};
		params.set(StringTextPairwiseParams.METRIC, HasMetric.Metric.LCS_SIM);
		mapper = new TextSimilarityPairwiseMapper(dataSchema, params);
		for (int i = 0; i < array.length; i++) {
			res[i] = (double) mapper.map(new Object[] {array[i].getField(0), array[i].getField(1)});
		}
		assertArrayEquals(lcsSim, res);

		// SSK
		Double[] ssk = new Double[] {0.0, 0.0, 0.0, 0.0};
		params.set(StringTextPairwiseParams.METRIC, HasMetric.Metric.SSK).set(HasKDefaultAs2.K, 1);
		mapper = new TextSimilarityPairwiseMapper(dataSchema, params);
		for (int i = 0; i < array.length; i++) {
			res[i] = (double) mapper.map(new Object[] {array[i].getField(0), array[i].getField(1)});
		}
		assertArrayEquals(ssk, res);

		// COSINE
		Double[] cosine = new Double[] {1.0, 0.0, 0.5, 0.5};
		params.set(StringTextPairwiseParams.METRIC, HasMetric.Metric.COSINE).set(HasKDefaultAs2.K, 1);
		mapper = new TextSimilarityPairwiseMapper(dataSchema, params);
		for (int i = 0; i < array.length; i++) {
			res[i] = (double) mapper.map(new Object[] {array[i].getField(0), array[i].getField(1)});
		}
		assertArrayEquals(cosine, res);

		// SIMHASH_HAMMING
		Double[] simHash = new Double[] {0.0, 29.0, 19.0, 15.0};
		params.set(StringTextPairwiseParams.METRIC, HasMetric.Metric.SIMHASH_HAMMING).set(HasKDefaultAs2.K, 1);
		mapper = new TextSimilarityPairwiseMapper(dataSchema, params);
		for (int i = 0; i < array.length; i++) {
			res[i] = (double) mapper.map(new Object[] {array[i].getField(0), array[i].getField(1)});
		}
		assertArrayEquals(simHash, res);

		// SIMHASH_HAMMING_SIM
		Double[] simHashSim = new Double[] {1.0, 0.546875, 0.703125, 0.765625};
		params.set(StringTextPairwiseParams.METRIC, HasMetric.Metric.SIMHASH_HAMMING_SIM).set(HasKDefaultAs2.K, 1);
		mapper = new TextSimilarityPairwiseMapper(dataSchema, params);
		for (int i = 0; i < array.length; i++) {
			res[i] = (double) mapper.map(new Object[] {array[i].getField(0), array[i].getField(1)});
		}
		assertArrayEquals(simHashSim, res);

		thrown.expect(RuntimeException.class);
		mapper.map(new Object[] {"s1", "s2", "s3"});
	}

}