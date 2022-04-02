package com.alibaba.alink.pipeline.similarity;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.dataproc.SortUtils.RowComparator;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class StringSimilarityPairwiseTest extends AlinkTestBase {

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

		list.sort(new RowComparator(0));

		Row[] output =
			new Row[] {
				Row.of(1L, "北京", "北京", 1.0),
				Row.of(2L, "北京欢迎", "中国人民", 0.0),
				Row.of(3L, "Beijing", "Beijing", 1.0),
				Row.of(4L, "Beijing", "Chinese", 0.0),
				Row.of(5L, "Good Morning!", "Good Evening!", 0.4),
			};

		assertListRowEqual(Arrays.asList(output), list, 0);
	}

	@Test
	public void testStream() throws Exception {
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

		StreamOperator <?> words = new MemSourceStreamOp(Arrays.asList(array),
			new String[] {"ID", selectedColName0, selectedColName1});
		StringSimilarityPairwise evalOp =
			new StringSimilarityPairwise()
				.setSelectedCols(new String[] {selectedColName0, selectedColName1})
				.setMetric("COSINE")
				.setOutputCol("COSINE")
				.setWindowSize(4);

		StreamOperator <?> res = evalOp.transform(words);
		CollectSinkStreamOp collectSinkStreamOp = new CollectSinkStreamOp()
			.linkFrom(res);
		StreamOperator.execute();

		List <Row> list = collectSinkStreamOp.getAndRemoveValues();

		list.sort(new RowComparator(0));

		Row[] output =
			new Row[] {
				Row.of(1L, "北京", "北京", 1.0),
				Row.of(2L, "北京欢迎", "中国人民", 0.0),
				Row.of(3L, "Beijing", "Beijing", 1.0),
				Row.of(4L, "Beijing", "Chinese", 0.0),
				Row.of(5L, "Good Morning!", "Good Evening!", 0.4),
			};

		assertListRowEqual(Arrays.asList(output), list, 0);
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
		StringSimilarityPairwise stringSimilarityPairwise = new StringSimilarityPairwise().setSelectedCols("text1",
			"text2").setMetric(
			"LEVENSHTEIN").setOutputCol("output");
		stringSimilarityPairwise.transform(inOp1).print();

		CollectSinkStreamOp collectSinkStreamOp = new CollectSinkStreamOp()
			.linkFrom(stringSimilarityPairwise.transform(inOp2));
		StreamOperator.execute();
		List <Row> list = collectSinkStreamOp.getAndRemoveValues();
		list.sort(new RowComparator(0));
		for (int i = 0; i < list.size(); i++) {
			System.out.println("\"" + list.get(i).toString() + "\",");
		}
	}

	@Test
	public void testLEVENSHTEINBatch() {
		List <Row> df = Arrays.asList(
			Row.of(0, "abcde", "aabce"),
			Row.of(1, "aacedw", "aabbed"),
			Row.of(2, "cdefa", "bbcefa"),
			Row.of(3, "bdefh", "ddeac"),
			Row.of(4, "acedm", "aeefbc")
		);
		BatchOperator <?> inOp1 = new MemSourceBatchOp(df, "id int, text1 string, text2 string");
		StringSimilarityPairwise stringSimilarityPairwise = new StringSimilarityPairwise().setSelectedCols("text1",
			"text2").setMetric(
			"LEVENSHTEIN").setOutputCol("output");

		List <Row> list = stringSimilarityPairwise.transform(inOp1).collect();
		list.sort(new RowComparator(0));

		List <Row> output = Arrays.asList(
			Row.of(0, "abcde", "aabce", 2.0),
			Row.of(1, "aacedw", "aabbed", 3.0),
			Row.of(2, "cdefa", "bbcefa", 3.0),
			Row.of(3, "bdefh", "ddeac", 3.0),
			Row.of(4, "acedm", "aeefbc", 4.0)
		);

		assertListRowEqual(output, list, 0);
	}

	@Test
	public void testLEVENSHTEINStream() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "abcde", "aabce"),
			Row.of(1, "aacedw", "aabbed"),
			Row.of(2, "cdefa", "bbcefa"),
			Row.of(3, "bdefh", "ddeac"),
			Row.of(4, "acedm", "aeefbc")
		);
		StreamOperator <?> inOp1 = new MemSourceStreamOp(df, "id int, text1 string, text2 string");
		StringSimilarityPairwise stringSimilarityPairwise = new StringSimilarityPairwise().setSelectedCols("text1",
			"text2").setMetric(
			"LEVENSHTEIN").setOutputCol("output");
		CollectSinkStreamOp collectSinkStreamOp = new CollectSinkStreamOp()
			.linkFrom(stringSimilarityPairwise.transform(inOp1));
		StreamOperator.execute();

		List <Row> list = collectSinkStreamOp.getAndRemoveValues();
		list.sort(new RowComparator(0));

		List <Row> output = Arrays.asList(
			Row.of(0, "abcde", "aabce", 2.0),
			Row.of(1, "aacedw", "aabbed", 3.0),
			Row.of(2, "cdefa", "bbcefa", 3.0),
			Row.of(3, "bdefh", "ddeac", 3.0),
			Row.of(4, "acedm", "aeefbc", 4.0)
		);

		assertListRowEqual(output, list, 0);
	}
}