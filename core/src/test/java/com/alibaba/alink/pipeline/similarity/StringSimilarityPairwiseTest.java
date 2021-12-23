package com.alibaba.alink.pipeline.similarity;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
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
			"+I[1, 北京, 北京, 1.0]",
			"+I[2, 北京欢迎, 中国人民, 0.0]",
			"+I[3, Beijing, Beijing, 1.0]",
			"+I[4, Beijing, Chinese, 0.0]",
			"+I[5, Good Morning!, Good Evening!, 0.4]"
		};

		String[] results = new String[list.size()];
		for (int i = 0; i < list.size(); i++) {
			results[i] = list.get(i).toString();
		}
		assertArrayEquals(output, results);
	}

	@Test
	public void testStringSimilarityPairwise() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "abcde", "aabce"),
			Row.of(1, "aacedw", "aabbed"),
			Row.of(2, "cdefa", "bbcefa"),
			Row.of(3, "bdefh", "ddeac"),
			Row.of(4, "acedm", "aeefbc")
		);
		BatchOperator <?> inOp1 = new MemSourceBatchOp(df, "id int, text1 string, text2 string");
		StreamOperator <?> inOp2 = new MemSourceStreamOp(df, "id int, text1 string, text2 string");
		StringSimilarityPairwise stringSimilarityPairwise = new StringSimilarityPairwise().setSelectedCols("text1", "text2").setMetric(
			"LEVENSHTEIN").setOutputCol("output");
		stringSimilarityPairwise.transform(inOp1).print();

		stringSimilarityPairwise.transform(inOp2).print();
		StreamOperator.execute();
	}
}
