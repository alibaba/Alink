package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.similarity.StringSimilarityPairwiseStreamOp;
import com.alibaba.alink.operator.stream.similarity.TextSimilarityPairwiseStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public class StringSimilarityPairwiseBatchOpTest extends AlinkTestBase {
	private String selectedColName0 = "col0";
	private String selectedColName1 = "col1";

	@Test
	public void testStringSimilarityBatch() {
		Row[] array =
			new Row[] {
				Row.of(1, "北京", "北京"),
				Row.of(2, "北京欢迎", "中国人民"),
				Row.of(3, "Beijing", "Beijing"),
				Row.of(4, "Beijing", "Chinese"),
				Row.of(5, "Good Morning!", "Good Evening!"),
			};
		MemSourceBatchOp words = new MemSourceBatchOp(Arrays.asList(array),
			new String[] {"ID", selectedColName0, selectedColName1});
		StringSimilarityPairwiseBatchOp evalOp =
			new StringSimilarityPairwiseBatchOp()
				.setSelectedCols(new String[] {selectedColName0, selectedColName1})
				//.setMetric("LEVENSHTEIN_SIM")
				//.setOutputCol("LEVENSHTEIN_SIM")
				.setMetric("COSINE")
				.setOutputCol("COSINE")
				.setWindowSize(4);
		List <Row> res = evalOp.linkFrom(words).collect();
		String[] output = {"1,北京,北京,1.0", "2,北京欢迎,中国人民,0.0", "3,Beijing,Beijing,1.0", "4,Beijing,Chinese,0.0",
			"5,Good Morning!,Good Evening!,0.4"};
		String[] results = new String[res.size()];
		for (int i = 0; i < res.size(); i++) {
			results[i] = res.get(i).toString();
		}
		assertArrayEquals(output, results);
	}

	@Test
	public void testStringSimilarityStream() throws Exception {
		Row[] array =
			new Row[] {
				Row.of(1, "北京", "北京"),
				Row.of(2, "北京欢迎", "中国人民"),
				Row.of(3, "Beijing", "Beijing"),
				Row.of(4, "Beijing", "Chinese"),
				Row.of(5, "Good Morning!", "Good Evening!")
			};
		MemSourceStreamOp words = new MemSourceStreamOp(Arrays.asList(array),
			new String[] {"ID", selectedColName0, selectedColName1});
		StringSimilarityPairwiseStreamOp evalOp =
			new StringSimilarityPairwiseStreamOp()
				.setSelectedCols(new String[] {selectedColName0, selectedColName1})
				.setMetric("COSINE")
				.setWindowSize(4)
				.setOutputCol("COSINE");

		TextSimilarityPairwiseStreamOp op =
			new TextSimilarityPairwiseStreamOp()
				.setSelectedCols(new String[] {selectedColName0, selectedColName1})
				.setMetric("COSINE")
				.setWindowSize(4)
				.setOutputCol(selectedColName0);

		op.linkFrom(words).print();
		StreamOperator.execute();
	}

}