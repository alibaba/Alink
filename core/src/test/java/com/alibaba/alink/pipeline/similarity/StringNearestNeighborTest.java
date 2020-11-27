package com.alibaba.alink.pipeline.similarity;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.similarity.StringNearestNeighborBatchOpTest;
import com.alibaba.alink.operator.batch.similarity.TextApproxNearestNeighborBatchOpTest;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.operator.batch.similarity.StringNearestNeighborBatchOpTest.extractScore;

public class StringNearestNeighborTest extends AlinkTestBase {
	@Test
	public void testString() {
		BatchOperator dict = new MemSourceBatchOp(Arrays.asList(StringNearestNeighborBatchOpTest.dictRows),
			new String[] {"id", "str"});
		BatchOperator query = new MemSourceBatchOp(Arrays.asList(StringNearestNeighborBatchOpTest.queryRows),
			new String[] {"id", "str"});

		BatchOperator neareastNeighbor = new StringNearestNeighbor()
			.setIdCol("id")
			.setSelectedCol("str")
			.setTopN(3)
			.setOutputCol("output")
			.fit(dict)
			.transform(query);

		List <Row> res = neareastNeighbor.collect();

		Map <Object, Double[]> score = new HashMap <>();
		score.put(1, new Double[] {0.75, 0.667, 0.333});
		score.put(2, new Double[] {0.667, 0.667, 0.5});
		score.put(3, new Double[] {0.333, 0.333, 0.25});
		score.put(4, new Double[] {0.75, 0.333, 0.333});
		score.put(5, new Double[] {0.333, 0.25, 0.25});
		score.put(6, new Double[] {0.333, 0.333, 0.333});

		for (Row row : res) {
			Double[] actual = StringNearestNeighborBatchOpTest.extractScore((String) row.getField(2));
			Double[] expect = score.get(row.getField(0));
			for (int i = 0; i < actual.length; i++) {
				Assert.assertEquals(actual[i], expect[i], 0.01);
			}
		}
	}

	@Test
	public void testStringApprox() {
		BatchOperator dict = new MemSourceBatchOp(Arrays.asList(StringNearestNeighborBatchOpTest.dictRows),
			new String[] {"id", "str"});
		BatchOperator query = new MemSourceBatchOp(Arrays.asList(StringNearestNeighborBatchOpTest.queryRows),
			new String[] {"id", "str"});

		BatchOperator neareastNeighbor = new StringApproxNearestNeighbor()
			.setIdCol("id")
			.setSelectedCol("str")
			.setTopN(3)
			.setOutputCol("output")
			.fit(dict)
			.transform(query);

		List <Row> res = neareastNeighbor.collect();

		Map <Object, Double[]> score = new HashMap <>();
		score.put(1, new Double[] {0.984375, 0.953125, 0.9375});
		score.put(2, new Double[] {0.984375, 0.953125, 0.9375});
		score.put(3, new Double[] {0.921875, 0.875, 0.875});
		score.put(4, new Double[] {0.9375, 0.890625, 0.8125});
		score.put(5, new Double[] {0.890625, 0.84375, 0.8125});
		score.put(6, new Double[] {0.9375, 0.890625, 0.8125});

		for (Row row : res) {
			Double[] actual = extractScore((String) row.getField(2));
			Double[] expect = score.get(row.getField(0));
			for (int i = 0; i < actual.length; i++) {
				Assert.assertEquals(actual[i], expect[i], 0.01);
			}
		}
	}

	@Test
	public void testText() {
		BatchOperator dict = new MemSourceBatchOp(Arrays.asList(TextApproxNearestNeighborBatchOpTest.dictRows),
			new String[] {"id", "str"});
		BatchOperator query = new MemSourceBatchOp(Arrays.asList(TextApproxNearestNeighborBatchOpTest.queryRows),
			new String[] {"id", "str"});

		BatchOperator neareastNeighbor = new TextNearestNeighbor()
			.setIdCol("id")
			.setSelectedCol("str")
			.setTopN(3)
			.setOutputCol("output")
			.fit(dict)
			.transform(query);

		List <Row> res = neareastNeighbor.collect();

		Map <Object, Double[]> score = new HashMap <>();
		score.put(1, new Double[] {0.75, 0.667, 0.333});
		score.put(2, new Double[] {0.667, 0.667, 0.5});
		score.put(3, new Double[] {0.333, 0.333, 0.25});
		score.put(4, new Double[] {0.75, 0.333, 0.333});
		score.put(5, new Double[] {0.333, 0.25, 0.25});
		score.put(6, new Double[] {0.333, 0.333, 0.333});

		for (Row row : res) {
			Double[] actual = StringNearestNeighborBatchOpTest.extractScore((String) row.getField(2));
			Double[] expect = score.get(row.getField(0));
			for (int i = 0; i < actual.length; i++) {
				Assert.assertEquals(actual[i], expect[i], 0.01);
			}
		}
	}

	@Test
	public void testTextApprox() {
		BatchOperator dict = new MemSourceBatchOp(Arrays.asList(TextApproxNearestNeighborBatchOpTest.dictRows),
			new String[] {"id", "str"});
		BatchOperator query = new MemSourceBatchOp(Arrays.asList(TextApproxNearestNeighborBatchOpTest.queryRows),
			new String[] {"id", "str"});

		BatchOperator neareastNeighbor = new TextApproxNearestNeighbor()
			.setIdCol("id")
			.setSelectedCol("str")
			.setTopN(3)
			.setOutputCol("output")
			.fit(dict)
			.transform(query);

		List <Row> res = neareastNeighbor.collect();

		Map <Object, Double[]> score = new HashMap <>();
		score.put(1, new Double[] {0.984375, 0.953125, 0.9375});
		score.put(2, new Double[] {0.984375, 0.953125, 0.9375});
		score.put(3, new Double[] {0.921875, 0.875, 0.875});
		score.put(4, new Double[] {0.9375, 0.890625, 0.8125});
		score.put(5, new Double[] {0.890625, 0.84375, 0.8125});
		score.put(6, new Double[] {0.9375, 0.890625, 0.8125});

		for (Row row : res) {
			Double[] actual = StringNearestNeighborBatchOpTest.extractScore((String) row.getField(2));
			Double[] expect = score.get(row.getField(0));
			for (int i = 0; i < actual.length; i++) {
				Assert.assertEquals(actual[i], expect[i], 0.01);
			}
		}
	}

	public static Row[] dictRows = new Row[] {
		Row.of("dict1", "0 0 0"),
		Row.of("dict2", "0.1 0.1 0.1"),
		Row.of("dict3", "0.2 0.2 0.2"),
		Row.of("dict4", "9 9 9"),
		Row.of("dict5", "9.1 9.1 9.1"),
		Row.of("dict6", "9.2 9.2 9.2")
	};
	public static Row[] queryRows = new Row[] {
		Row.of(1, "0 0 0"),
		Row.of(2, "0.1 0.1 0.1"),
		Row.of(3, "0.2 0.2 0.2"),
		Row.of(4, "9 9 9"),
		Row.of(5, "9.1 9.1 9.1"),
		Row.of(6, "9.2 9.2 9.2")
	};

	@Test
	public void testVector() {
		BatchOperator dict = new MemSourceBatchOp(Arrays.asList(dictRows), new String[] {"id", "vec"});
		BatchOperator query = new MemSourceBatchOp(Arrays.asList(queryRows), new String[] {"id", "vec"});

		BatchOperator neareastNeighbor = new VectorNearestNeighbor()
			.setIdCol("id")
			.setSelectedCol("vec")
			.setTopN(3)
			.setOutputCol("output")
			.fit(dict)
			.transform(query);

		List <Row> res = neareastNeighbor.collect();

		Map <Object, Double[]> score = new HashMap <>();
		score.put(1, new Double[] {0.0, 0.17320508075688776, 0.3464101615137755});
		score.put(2, new Double[] {0.0, 0.17320508075688773, 0.17320508075688776});
		score.put(3, new Double[] {0.0, 0.17320508075688776, 0.3464101615137755});
		score.put(4, new Double[] {0.0, 0.17320508075680896, 0.346410161513782});
		score.put(5, new Double[] {0.0, 0.17320508075680896, 0.17320508075680896});
		score.put(6, new Double[] {0.0, 0.17320508075680896, 0.346410161513782});

		for (Row row : res) {
			Double[] actual = StringNearestNeighborBatchOpTest.extractScore((String) row.getField(2));
			Double[] expect = score.get(row.getField(0));
			for (int i = 0; i < actual.length; i++) {
				Assert.assertEquals(actual[i], expect[i], 0.01);
			}
		}
	}

	@Test
	public void testVectorApprox() {
		BatchOperator dict = new MemSourceBatchOp(Arrays.asList(dictRows), new String[] {"id", "vec"});
		BatchOperator query = new MemSourceBatchOp(Arrays.asList(queryRows), new String[] {"id", "vec"});

		BatchOperator neareastNeighbor = new VectorApproxNearestNeighbor()
			.setIdCol("id")
			.setSelectedCol("vec")
			.setTopN(3)
			.setOutputCol("output")
			.fit(dict)
			.transform(query);

		List <Row> res = neareastNeighbor.collect();

		Map <Object, Double[]> score = new HashMap <>();
		score.put(1, new Double[] {0.0, 0.17320508075688776, 0.3464101615137755});
		score.put(2, new Double[] {0.0, 0.17320508075688773, 0.17320508075688776});
		score.put(3, new Double[] {0.0, 0.17320508075688776, 0.3464101615137755});
		score.put(4, new Double[] {0.0, 0.17320508075680896, 0.346410161513782});
		score.put(5, new Double[] {0.0, 0.17320508075680896, 0.17320508075680896});
		score.put(6, new Double[] {0.0, 0.17320508075680896, 0.346410161513782});

		for (Row row : res) {
			Double[] actual = StringNearestNeighborBatchOpTest.extractScore((String) row.getField(2));
			Double[] expect = score.get(row.getField(0));
			for (int i = 0; i < actual.length; i++) {
				Assert.assertEquals(actual[i], expect[i], 0.01);
			}
		}
	}
}