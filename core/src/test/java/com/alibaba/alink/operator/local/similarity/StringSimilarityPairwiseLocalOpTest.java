package com.alibaba.alink.operator.local.similarity;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.Utils;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class StringSimilarityPairwiseLocalOpTest extends TestCase {
	private String selectedColName0 = "col0";
	private String selectedColName1 = "col1";

	@Test
	public void test() throws Exception {
		Row[] array =
			new Row[] {
				Row.of(1L, "北京", "北京"),
				Row.of(2L, "北京欢迎", "中国人民"),
				Row.of(3L, "Beijing", "Beijing"),
				Row.of(4L, "Beijing", "Chinese"),
				Row.of(5L, "Good Morning!", "Good Evening!"),
			};
		LocalOperator words = new MemSourceLocalOp(Arrays.asList(array),
			new String[] {"ID", selectedColName0, selectedColName1});
		StringSimilarityPairwiseLocalOp evalOp =
			new StringSimilarityPairwiseLocalOp()
				.setSelectedCols(new String[] {selectedColName0, selectedColName1})
				.setMetric("COSINE")
				.setOutputCol("COSINE")
				.setWindowSize(4);
		LocalOperator res = evalOp.linkFrom(words);

		List <Row> list = res.getOutputTable().getRows();

		Collections.sort(list, new Comparator <Row>() {
			@Override
			public int compare(Row o1, Row o2) {
				return Long.compare((long) o1.getField(0), (long) o2.getField(0));
			}
		});

		Row[] output =
			new Row[] {
				Row.of(1L, "北京", "北京", 1.0),
				Row.of(2L, "北京欢迎", "中国人民", 0.0),
				Row.of(3L, "Beijing", "Beijing", 1.0),
				Row.of(4L, "Beijing", "Chinese", 0.0),
				Row.of(5L, "Good Morning!", "Good Evening!", 0.4),
			};

		Utils.assertListRowEqual(Arrays.asList(output), list, 0);
	}
}