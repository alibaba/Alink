package com.alibaba.alink.pipeline.similarity;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public class StringSimilarityPairwiseTest {

	@Test
	public void test() {
		Row[] array =
			new Row[] {
				Row.of(1L, "北京", "北京"),
				Row.of(2L, "北京欢迎", "中国人民"),
				Row.of(3L, "Beijing", "Beijing"),
				Row.of(4L, "Beijing", "Chinese"),
				Row.of(5L, "Good Morning!", "Good Evening!"),
			};

		String selectedColName0 = "col0";
		String selectedColName1 = "col1";

		BatchOperator <?> words = new MemSourceBatchOp(Arrays.asList(array),
			new String[] {"ID", selectedColName0, selectedColName1});
		StringSimilarityPairwise evalOp =
			new StringSimilarityPairwise()
				.setSelectedCols(new String[] {selectedColName0, selectedColName1})
				.setMetric("COSINE")
				.setOutputCol("COSINE")
				.setWindowSize(4);

		BatchOperator <?> res = evalOp.transform(words);

		List <Row> list = res.collect();

		list.sort(Comparator.comparingLong(o -> (long) o.getField(0)));

		String[] output = {
			"1,北京,北京,1.0",
			"2,北京欢迎,中国人民,0.0",
			"3,Beijing,Beijing,1.0",
			"4,Beijing,Chinese,0.0",
			"5,Good Morning!,Good Evening!,0.4"
		};

		String[] results = new String[list.size()];
		for (int i = 0; i < list.size(); i++) {
			results[i] = list.get(i).toString();
		}
		assertArrayEquals(output, results);
	}
}