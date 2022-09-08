package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KCoreBatchOpTest extends AlinkTestBase {

	private List <Row> intInputRows;

	@Before
	public void before() {
		intInputRows = Arrays.asList(
			Row.of(1, 2),
			Row.of(1, 3),
			Row.of(1, 4),
			Row.of(2, 3),
			Row.of(2, 4),
			Row.of(3, 4),
			Row.of(3, 5),
			Row.of(3, 6),
			Row.of(5, 6),
			Row.of(6, 7)
		);
	}

	@Test
	public void testIntInputK1() {
		List <Row> expectedOutput = Arrays.asList(
			Row.of(1, 2),
			Row.of(2, 1),
			Row.of(1, 3),
			Row.of(3, 1),
			Row.of(1, 4),
			Row.of(4, 1),
			Row.of(2, 3),
			Row.of(3, 2),
			Row.of(2, 4),
			Row.of(4, 2),
			Row.of(3, 4),
			Row.of(4, 3),
			Row.of(3, 5),
			Row.of(5, 3),
			Row.of(3, 6),
			Row.of(6, 3),
			Row.of(5, 6),
			Row.of(6, 5)
		);
		verifyExecution(intInputRows, expectedOutput, 1);
	}

	@Test
	public void testIntInputK2() {
		List <Row> expectedOutputK2 = Arrays.asList(
			Row.of(1, 2),
			Row.of(2, 1),
			Row.of(1, 3),
			Row.of(3, 1),
			Row.of(1, 4),
			Row.of(4, 1),
			Row.of(2, 3),
			Row.of(3, 2),
			Row.of(2, 4),
			Row.of(4, 2),
			Row.of(3, 4),
			Row.of(4, 3)
		);
		verifyExecution(intInputRows, expectedOutputK2, 2);
	}

	@Test
	public void testDuplicateEdge() {
		List <Row> duplicateInput = Arrays.asList(
			Row.of(1, 2),
			Row.of(1, 2),
			Row.of(2, 3),
			Row.of(2, 4),
			Row.of(3, 4),
			Row.of(3, 4)
		);

		List <Row> expectedOutputK2 = new ArrayList <>();
		verifyExecution(duplicateInput, expectedOutputK2, 2);
	}

	@Test
	public void testIntInputK3() {
		List <Row> expectedOutputK2 = new ArrayList <>();
		verifyExecution(intInputRows, expectedOutputK2, 3);
	}

	@Test
	public void testLongInputK1() {
		List <Row> inputRows = Arrays.asList(
			Row.of(1L, 2L),
			Row.of(1L, 3L),
			Row.of(1L, 4L),
			Row.of(2L, 3L)
		);
		List <Row> expectedOutput = Arrays.asList(
			Row.of(1L, 2L),
			Row.of(2L, 1L),
			Row.of(1L, 3L),
			Row.of(3L, 1L),
			Row.of(2L, 3L),
			Row.of(3L, 2L));
		verifyExecution(inputRows, expectedOutput, 1);
	}

	@Test
	public void testStringInputK1() {
		List <Row> inputRows = Arrays.asList(
			Row.of("1", "2"),
			Row.of("1", "3"),
			Row.of("1", "4"),
			Row.of("2", "3")
		);
		List <Row> expectedOutput = Arrays.asList(
			Row.of("1", "2"),
			Row.of("2", "1"),
			Row.of("1", "3"),
			Row.of("3", "1"),
			Row.of("2", "3"),
			Row.of("3", "2"));
		verifyExecution(inputRows, expectedOutput, 1);
	}

	@Test
	public void testDoubleInputK1() {
		List <Row> inputRows = Arrays.asList(
			Row.of(1., 2.),
			Row.of(1., 3.),
			Row.of(1., 4.),
			Row.of(2., 3.)
		);
		BatchOperator <?> input = new MemSourceBatchOp(inputRows, new String[] {"source", "target"});
		try {
			new KCoreBatchOp()
				.setEdgeSourceCol("source")
				.setEdgeTargetCol("target")
				.setK(1)
				.linkFrom(input);
			Assert.fail();
		} catch (Exception e) {
			Assert.assertEquals(
				"Unsupported vertex type. Supported types are [int, long, string].", e.getMessage());
		}
	}

	private void verifyExecution(List <Row> inputRows, List <Row> expectedOutput, int k) {
		BatchOperator <?> input = new MemSourceBatchOp(
			inputRows,
			new String[] {"source", "target"});

		KCoreBatchOp op = new KCoreBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.setK(k)
			.linkFrom(input);
		List <Row> output = op.collect();

		compareResultCollections(expectedOutput, output, (o1, o2) -> {
			int cmp = String.valueOf(o1.getField(0)).compareTo(String.valueOf(o2.getField(0)));
			if (cmp == 0) {
				return String.valueOf(o1.getField(1)).compareTo(String.valueOf(o2.getField(1)));
			} else {
				return cmp;
			}
		});
	}
}