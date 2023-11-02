package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CommonNeighborsBatchOpTest extends AlinkTestBase {

	private List<Tuple5 <String, String, Long, Double, Double>> rowToTuple(List<Row> rows) {
		List<Tuple5 <String, String, Long, Double, Double>> list = new ArrayList <>();
		for (Row row : rows) {
			list.add(new Tuple5<> (
				String.valueOf(row.getField(0)),
				String.valueOf(row.getField(1)),
				(Long) row.getField(3),
				Math.round((Double) row.getField(4) * 10000) / 10000.0,
				Math.round((Double) row.getField(5) * 10000) / 10000.0
			));
		}
		return list;
	}

	@Test
	public void testBipartiteGraph() throws Exception {
		Row[] rows = new Row[]{
			Row.of("a1", "11L"),
			Row.of("a1", "12L"),
			Row.of("a1", "16L"),
			Row.of("a2", "11L"),
			Row.of("a2", "12L"),
			Row.of("a3", "12L"),
			Row.of("a3", "13L")
		};

		List< Tuple5 <String, String, Long, Double, Double>> expectedResultList = new ArrayList <>();
		expectedResultList.add(new Tuple5("a2", "a3", 1L, 0.3333, 0.9102));
		expectedResultList.add(new Tuple5("a2", "a1", 2L, 0.6667, 2.3529));
		expectedResultList.add(new Tuple5("a3", "a1", 1L, 0.25, 0.9102));
		expectedResultList.add(new Tuple5("a3", "a2", 1L, 0.3333, 0.9102));
		expectedResultList.add(new Tuple5("a1", "a2", 2L, 0.6667, 2.3529));
		expectedResultList.add(new Tuple5("a1", "a3", 1L, 0.2500, 0.9102));

		String[] colNames = new String[]{"user", "item"};
		BatchOperator inputdata = new MemSourceBatchOp(
			Arrays.asList(rows), colNames);
		AlinkGlobalConfiguration.setPrintProcessInfo(true);

		CommonNeighborsBatchOp cn = new CommonNeighborsBatchOp()
			.setEdgeSourceCol("user")
			.setEdgeTargetCol("item")
			.setIsBipartiteGraph(true)
			.linkFrom(inputdata);
		cn.lazyPrint(-1);
		List < Tuple5 <String, String, Long, Double, Double>> result = rowToTuple(cn.collect());
		compareResultCollections(
			result,
			expectedResultList,
			new TupleComparator<Tuple5 <String, String, Long, Double, Double>>()
			);
	}

	@Test
	public void testGraph() throws Exception {
		Row[] rows = new Row[]{
			Row.of("a1", "11L"),
			Row.of("a1", "12L"),
			Row.of("a1", "16L"),
			Row.of("a2", "11L"),
			Row.of("a2", "12L"),
			Row.of("a3", "12L"),
			Row.of("a3", "13L")
		};

		List< Tuple5 <String, String, Long, Double, Double>> expectedResultList = new ArrayList <>();
		expectedResultList.add(new Tuple5("a2", "a3", 1L, 0.3333, 0.9102));
		expectedResultList.add(new Tuple5("a2", "a1", 2L, 0.6667, 2.3529));
		expectedResultList.add(new Tuple5("a3", "a1", 1L, 0.25, 0.9102));
		expectedResultList.add(new Tuple5("a3", "a2", 1L, 0.3333, 0.9102));
		expectedResultList.add(new Tuple5("a1", "a2", 2L, 0.6667, 2.3529));
		expectedResultList.add(new Tuple5("a1", "a3", 1L, 0.2500, 0.9102));
		expectedResultList.add(new Tuple5("16L", "11L", 1L, 0.5000, 0.9102));
		expectedResultList.add(new Tuple5("16L", "12L", 1L, 0.3333, 0.9102));
		expectedResultList.add(new Tuple5("12L", "16L", 1L, 0.3333, 0.9102));
		expectedResultList.add(new Tuple5("11L", "16L", 1L, 0.5000, 0.9102));
		expectedResultList.add(new Tuple5("13L", "12L", 1L, 0.3333, 1.4427));
		expectedResultList.add(new Tuple5("11L", "12L", 2L, 0.6667, 2.3529));
		expectedResultList.add(new Tuple5("12L", "13L", 1L, 0.3333, 1.4427));
		expectedResultList.add(new Tuple5("12L", "11L", 2L, 0.6667, 2.3529));

		String[] colNames = new String[]{"user", "item"};
		BatchOperator inputdata = new MemSourceBatchOp(
			Arrays.asList(rows), colNames);
		AlinkGlobalConfiguration.setPrintProcessInfo(true);

		CommonNeighborsBatchOp cn = new CommonNeighborsBatchOp()
			.setEdgeSourceCol("user")
			.setEdgeTargetCol("item")
			.setIsBipartiteGraph(false)
			.linkFrom(inputdata);
		cn.lazyPrint(-1);
		List < Tuple5 <String, String, Long, Double, Double>> result = rowToTuple(cn.collect());
		compareResultCollections(
			result,
			expectedResultList,
			new TupleComparator<Tuple5 <String, String, Long, Double, Double>>()
		);
	}

	@Test
	public void testGraphWithLongInput() throws Exception {
		Row[] rows = new Row[]{
			Row.of(0L, 11L),
			Row.of(0L, 12L),
			Row.of(0L, 16L),
			Row.of(1L, 11L),
			Row.of(1L, 12L),
			Row.of(2L, 12L),
			Row.of(2L, 13L)
		};

		List< Tuple5 <String, String, Long, Double, Double>> expectedResultList = new ArrayList <>();
		expectedResultList.add(new Tuple5("1", "2", 1L, 0.3333, 0.9102));
		expectedResultList.add(new Tuple5("1", "0", 2L, 0.6667, 2.3529));
		expectedResultList.add(new Tuple5("2", "0", 1L, 0.25, 0.9102));
		expectedResultList.add(new Tuple5("2", "1", 1L, 0.3333, 0.9102));
		expectedResultList.add(new Tuple5("0", "1", 2L, 0.6667, 2.3529));
		expectedResultList.add(new Tuple5("0", "2", 1L, 0.2500, 0.9102));
		expectedResultList.add(new Tuple5("16", "11", 1L, 0.5000, 0.9102));
		expectedResultList.add(new Tuple5("16", "12", 1L, 0.3333, 0.9102));
		expectedResultList.add(new Tuple5("12", "16", 1L, 0.3333, 0.9102));
		expectedResultList.add(new Tuple5("11", "16", 1L, 0.5000, 0.9102));
		expectedResultList.add(new Tuple5("13", "12", 1L, 0.3333, 1.4427));
		expectedResultList.add(new Tuple5("11", "12", 2L, 0.6667, 2.3529));
		expectedResultList.add(new Tuple5("12", "13", 1L, 0.3333, 1.4427));
		expectedResultList.add(new Tuple5("12", "11", 2L, 0.6667, 2.3529));

		String[] colNames = new String[]{"user", "item"};
		BatchOperator inputdata = new MemSourceBatchOp(
			Arrays.asList(rows), colNames);
		AlinkGlobalConfiguration.setPrintProcessInfo(true);

		CommonNeighborsBatchOp cn = new CommonNeighborsBatchOp()
			.setEdgeSourceCol("user")
			.setEdgeTargetCol("item")
			.setIsBipartiteGraph(false)
			.setNeedTransformID(false)
			.linkFrom(inputdata);
		cn.lazyPrint(-1);
		List < Tuple5 <String, String, Long, Double, Double>> result = rowToTuple(cn.collect());
		compareResultCollections(
			result,
			expectedResultList,
			new TupleComparator<Tuple5 <String, String, Long, Double, Double>>()
		);
	}

	@Test
	public void testNoIDTransform() throws Exception {
		Row[] rows = new Row[]{
			Row.of(0L, 10L),
			Row.of(0L, 11L),
			Row.of(0L, 14L),
			Row.of(1L, 10L),
			Row.of(1L, 11L),
			Row.of(2L, 11L),
			Row.of(2L, 12L),
		};

		String[] colNames = new String[]{"user", "item"};
		BatchOperator inputdata = new MemSourceBatchOp(
			Arrays.asList(rows), colNames);
		AlinkGlobalConfiguration.setPrintProcessInfo(true);

		CommonNeighborsBatchOp cn = new CommonNeighborsBatchOp()
			.setEdgeSourceCol("user")
			.setEdgeTargetCol("item")
			.setNeedTransformID(false)
			.setIsBipartiteGraph(true)
			.linkFrom(inputdata);
		cn.lazyPrint(-1);

		List< Tuple5 <String, String, Long, Double, Double>> expectedResultList = new ArrayList <>();
		expectedResultList.add(new Tuple5("2", "1", 1L, 0.3333, 0.9102));
		expectedResultList.add(new Tuple5("1", "2", 1L, 0.3333, 0.9102));
		expectedResultList.add(new Tuple5("0", "1", 2L, 0.6667, 2.3529));
		expectedResultList.add(new Tuple5("2", "0", 1L, 0.2500, 0.9102));
		expectedResultList.add(new Tuple5("0", "2", 1L, 0.2500, 0.9102));
		expectedResultList.add(new Tuple5("1", "0", 2L, 0.6667, 2.3529));

		List < Tuple5 <String, String, Long, Double, Double>> result = rowToTuple(cn.collect());
		compareResultCollections(
			result,
			expectedResultList,
			new TupleComparator<Tuple5 <String, String, Long, Double, Double>>()
		);
	}
}
