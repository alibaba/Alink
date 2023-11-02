package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EdgeClusterCoefficientBatchOpTest extends AlinkTestBase {

	private List<Tuple6 <String, String, Long, Long, Long, Double>> rowToTuple(List<Row> rows) {
		List<Tuple6 <String, String, Long, Long, Long, Double>> list = new ArrayList <>();
		for (Row row : rows) {
			list.add(new Tuple6<> (
				String.valueOf(row.getField(0)),
				String.valueOf(row.getField(1)),
				(Long) row.getField(2),
				(Long) row.getField(3),
				(Long) row.getField(4),
				Math.round((Double) row.getField(5) * 10000) / 10000.0
			));
		}
		return list;
	}

	@Test
	public void test() throws Exception {
		//the input edge construct the directed graph,
		//while the EdgeClusterCoefficient algorithm requires undirected graph.
		Row[] rows = new Row[] {
			Row.of(new Object[] {1.0, 2.0, 0.1}),
			Row.of(new Object[] {1.0, 3.0, 0.1}),
			Row.of(new Object[] {3.0, 2.0, 0.1}),
			Row.of(new Object[] {5.0, 2.0, 0.1}),
			Row.of(new Object[] {3.0, 4.0, 0.1}),
			Row.of(new Object[] {4.0, 2.0, 0.1}),
			Row.of(new Object[] {5.0, 4.0, 0.1}),
			Row.of(new Object[] {5.0, 1.0, 0.1}),
			Row.of(new Object[] {5.0, 3.0, 0.1}),
			Row.of(new Object[] {5.0, 6.0, 0.1}),
			Row.of(new Object[] {5.0, 8.0, 0.1}),
			Row.of(new Object[] {7.0, 6.0, 0.1}),
			Row.of(new Object[] {7.0, 1.0, 0.1}),
			Row.of(new Object[] {7.0, 5.0, 0.1}),
			Row.of(new Object[] {8.0, 6.0, 0.1}),
			Row.of(new Object[] {8.0, 4.0, 0.1})};

		String[] colNames = new String[]{"source", "target", "weight"};
		BatchOperator inputdata = new MemSourceBatchOp(
			Arrays.asList(rows), colNames);
		BatchOperator res = new EdgeClusterCoefficientBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.linkFrom(inputdata);
		List<Tuple6 <String, String, Long, Long, Long, Double>> result = rowToTuple(res.collect());

		List<Tuple6 <String, String, Long, Long, Long, Double>> expectedResultList = new ArrayList <>();
		expectedResultList.add(new Tuple6 <>("1.0", "7.0", 4L, 3L, 1L, 0.3333));
		expectedResultList.add(new Tuple6 <>("6.0", "7.0", 3L, 3L, 1L, 0.3333));
		expectedResultList.add(new Tuple6 <>("5.0", "7.0", 7L, 3L, 2L, 0.6667));
		expectedResultList.add(new Tuple6 <>("3.0", "1.0", 4L, 4L, 2L, 0.5000));
		expectedResultList.add(new Tuple6 <>("2.0", "1.0", 4L, 4L, 2L, 0.5000));
		expectedResultList.add(new Tuple6 <>("5.0", "1.0", 7L, 4L, 3L, 0.7500));
		expectedResultList.add(new Tuple6 <>("4.0", "3.0", 4L, 4L, 2L, 0.5000));
		expectedResultList.add(new Tuple6 <>("5.0", "3.0", 7L, 4L, 3L, 0.7500));
		expectedResultList.add(new Tuple6 <>("8.0", "6.0", 3L, 3L, 1L, 0.3333));
		expectedResultList.add(new Tuple6 <>("4.0", "2.0", 4L, 4L, 2L, 0.5000));
		expectedResultList.add(new Tuple6 <>("5.0", "2.0", 7L, 4L, 3L, 0.7500));
		expectedResultList.add(new Tuple6 <>("3.0", "2.0", 4L, 4L, 3L, 0.7500));
		expectedResultList.add(new Tuple6 <>("5.0", "4.0", 7L, 4L, 3L, 0.7500));
		expectedResultList.add(new Tuple6 <>("8.0", "4.0", 3L, 4L, 1L, 0.3333));
		expectedResultList.add(new Tuple6 <>("8.0", "5.0", 3L, 7L, 2L, 0.6667));
		expectedResultList.add(new Tuple6 <>("6.0", "5.0", 3L, 7L, 2L, 0.6667));
		compareResultCollections(
			result,
			expectedResultList,
			new TupleComparator<Tuple6 <String, String, Long, Long, Long, Double>>()
		);
	}

	@Test
	public void testLongInput() throws Exception {
		//the input edge construct the directed graph,
		//while the EdgeClusterCoefficient algorithm requires undirected graph.
		Row[] rows = new Row[] {
			Row.of(new Object[] {1L, 2L, 0.1}),
			Row.of(new Object[] {1L, 3L, 0.1}),
			Row.of(new Object[] {3L, 2L, 0.1}),
			Row.of(new Object[] {5L, 2L, 0.1}),
			Row.of(new Object[] {3L, 4L, 0.1}),
			Row.of(new Object[] {4L, 2L, 0.1}),
			Row.of(new Object[] {5L, 4L, 0.1}),
			Row.of(new Object[] {5L, 1L, 0.1}),
			Row.of(new Object[] {5L, 3L, 0.1}),
			Row.of(new Object[] {5L, 6L, 0.1}),
			Row.of(new Object[] {5L, 8L, 0.1}),
			Row.of(new Object[] {7L, 6L, 0.1}),
			Row.of(new Object[] {7L, 1L, 0.1}),
			Row.of(new Object[] {7L, 5L, 0.1}),
			Row.of(new Object[] {8L, 6L, 0.1}),
			Row.of(new Object[] {8L, 4L, 0.1})};

		String[] colNames = new String[]{"source", "target", "weight"};
		BatchOperator inputdata = new MemSourceBatchOp(
			Arrays.asList(rows), colNames);
		BatchOperator res = new EdgeClusterCoefficientBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.linkFrom(inputdata);
		List<Tuple6 <String, String, Long, Long, Long, Double>> result = rowToTuple(res.collect());

		List<Tuple6 <String, String, Long, Long, Long, Double>> expectedResultList = new ArrayList <>();
		expectedResultList.add(new Tuple6 <>("2", "1", 4L, 4L, 2L, 0.5000));
		expectedResultList.add(new Tuple6 <>("5", "1", 7L, 4L, 3L, 0.7500));
		expectedResultList.add(new Tuple6 <>("3", "1", 4L, 4L, 2L, 0.5000));
		expectedResultList.add(new Tuple6 <>("7", "1", 3L, 4L, 1L, 0.3333));
		expectedResultList.add(new Tuple6 <>("5", "2", 7L, 4L, 3L, 0.7500));
		expectedResultList.add(new Tuple6 <>("4", "2", 4L, 4L, 2L, 0.5000));
		expectedResultList.add(new Tuple6 <>("3", "2", 4L, 4L, 3L, 0.7500));
		expectedResultList.add(new Tuple6 <>("6", "5", 3L, 7L, 2L, 0.6667));
		expectedResultList.add(new Tuple6 <>("7", "5", 3L, 7L, 2L, 0.6667));
		expectedResultList.add(new Tuple6 <>("3", "4", 4L, 4L, 2L, 0.5000));
		expectedResultList.add(new Tuple6 <>("8", "4", 3L, 4L, 1L, 0.3333));
		expectedResultList.add(new Tuple6 <>("5", "4", 7L, 4L, 3L, 0.7500));
		expectedResultList.add(new Tuple6 <>("5", "3", 7L, 4L, 3L, 0.7500));
		expectedResultList.add(new Tuple6 <>("6", "8", 3L, 3L, 1L, 0.3333));
		expectedResultList.add(new Tuple6 <>("5", "8", 7L, 3L, 2L, 0.6667));
		expectedResultList.add(new Tuple6 <>("7", "6", 3L, 3L, 1L, 0.3333));
		compareResultCollections(
			result,
			expectedResultList,
			new TupleComparator<Tuple6 <String, String, Long, Long, Long, Double>>()
		);
	}

	@Test
	public void testDirectedGraph() throws Exception {
		//the input edge construct the directed graph,
		//while the EdgeClusterCoefficient algorithm requires undirected graph.
		Row[] rows = new Row[] {
			Row.of(new Object[] {1.0, 2.0, 0.1}),
			Row.of(new Object[] {1.0, 3.0, 0.1}),
			Row.of(new Object[] {3.0, 2.0, 0.1}),
			Row.of(new Object[] {5.0, 2.0, 0.1}),
			Row.of(new Object[] {3.0, 4.0, 0.1}),
			Row.of(new Object[] {4.0, 2.0, 0.1}),
			Row.of(new Object[] {5.0, 4.0, 0.1}),
			Row.of(new Object[] {5.0, 1.0, 0.1}),
			Row.of(new Object[] {5.0, 3.0, 0.1}),
			Row.of(new Object[] {5.0, 6.0, 0.1}),
			Row.of(new Object[] {5.0, 8.0, 0.1}),
			Row.of(new Object[] {7.0, 6.0, 0.1}),
			Row.of(new Object[] {7.0, 1.0, 0.1}),
			Row.of(new Object[] {7.0, 5.0, 0.1}),
			Row.of(new Object[] {8.0, 6.0, 0.1}),
			Row.of(new Object[] {8.0, 4.0, 0.1})};

		String[] colNames = new String[]{"source", "target", "weight"};
		BatchOperator inputdata = new MemSourceBatchOp(
			Arrays.asList(rows), colNames);
		BatchOperator res = new EdgeClusterCoefficientBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.setAsUndirectedGraph(false)
			.linkFrom(inputdata);
		List<Tuple6 <String, String, Long, Long, Long, Double>> result = rowToTuple(res.collect());

		List<Tuple6 <String, String, Long, Long, Long, Double>> expectedResultList = new ArrayList <>();
		expectedResultList.add(new Tuple6 <>("5.0", "1.0", 1L, 2L, 1L, 1.0000));
		expectedResultList.add(new Tuple6 <>("5.0", "3.0", 1L, 2L, 0L, 0.0000));
		expectedResultList.add(new Tuple6 <>("8.0", "6.0", 1L, 3L, 1L, 1.0000));
		expectedResultList.add(new Tuple6 <>("4.0", "2.0", 3L, 4L, 2L, 0.6667));
		expectedResultList.add(new Tuple6 <>("5.0", "2.0", 1L, 4L, 0L, 0.0000));
		expectedResultList.add(new Tuple6 <>("3.0", "2.0", 2L, 4L, 2L, 1.0000));
		expectedResultList.add(new Tuple6 <>("5.0", "4.0", 1L, 3L, 0L, 0.0000));
		expectedResultList.add(new Tuple6 <>("8.0", "4.0", 1L, 3L, 1L, 1.0000));
		compareResultCollections(
			result,
			expectedResultList,
			new TupleComparator<Tuple6 <String, String, Long, Long, Long, Double>>()
		);
	}
}