package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;

import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PageRankBatchOpTest extends AlinkTestBase {

	@Test
	public void testStringInput() {
		Row[] inputRows = new Row[]{
			Row.of("D","A",1),
			Row.of("D","B",1),
			Row.of("B","C",1),
			Row.of("C","B",1),
			Row.of("E","D",1),
			Row.of("E","F",1),
			Row.of("E","B",1),
			Row.of("F","B",1),
			Row.of("F","E",1),
			Row.of("G","B",1),
			Row.of("G","E",1),
			Row.of("H","B",1),
			Row.of("H","E",1),
			Row.of("I","B",1),
			Row.of("I","E",1),
			Row.of("J","E",1),
			Row.of("K","E",1)
		};

		List <Row> expectedOutput = Arrays.asList(
			Row.of("A", 0.03278149),
			Row.of("B", 0.38440096),
			Row.of("C", 0.34291027),
			Row.of("D", 0.03908709),
			Row.of("E", 0.08088569),
			Row.of("F", 0.03908709),
			Row.of("G", 0.01616948),
			Row.of("H", 0.01616948),
			Row.of("I", 0.01616948),
			Row.of("J", 0.01616948),
			Row.of("K", 0.01616948)
		);
		verifyExecution(inputRows, expectedOutput);
	}

	@Test
	public void testIntInput() {
		Row[] inputRows = new Row[] {
			Row.of(1, 2, 1.0),
			Row.of(1, 3, 1.0),
			Row.of(2, 3, 1.0),
			Row.of(2, 4, 1.0),
			Row.of(3, 4, 1.0)
		};
		List <Row> expectedOutput = Arrays.asList(
			Row.of(1, 0.1284), Row.of(2, 0.1829), Row.of(3, 0.2607), Row.of(4, 0.4278));
		verifyExecution(inputRows, expectedOutput);
	}

	@Test
	public void testLongInput() {
		Row[] inputRows = new Row[] {
			Row.of(1L, 2L, 1.0),
			Row.of(1L, 3L, 1.0),
			Row.of(2L, 3L, 1.0),
			Row.of(2L, 4L, 1.0),
			Row.of(3L, 4L, 1.0)
		};
		List <Row> expectedOutput = Arrays.asList(
			Row.of(1L, 0.1284), Row.of(2L, 0.1829), Row.of(3L, 0.2607), Row.of(4L, 0.4278));
		verifyExecution(inputRows, expectedOutput);
	}

	private void verifyExecution(Row[] inputRows, List <Row> expectedOutput) {
		BatchOperator <?> input = new MemSourceBatchOp(inputRows, new String[] {"source", "target", "weight"});

		PageRankBatchOp pageRank = new PageRankBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.setEdgeWeightCol("weight")
			.setEpsilon(0.001)
			.linkFrom(input);
		List <Row> result = pageRank.collect();
		result.sort(Comparator.comparing(o -> (o.getField(0).toString())));
		expectedOutput.sort(Comparator.comparing(o -> o.getField(0).toString()));
		assertEquals(expectedOutput.size(), result.size());
		for (int i = 0; i < expectedOutput.size(); i++) {
			Row expectedRow = expectedOutput.get(i);
			Row resultRow = result.get(i);
			assertEquals((expectedRow.getField(0)), resultRow.getField(0));
			assertEquals((Double) (expectedRow.getField(1)), (Double) (resultRow.getField(1)), 1e-3);
		}
	}
}