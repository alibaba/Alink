package com.alibaba.alink.operator.batch.graph.utils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The result of ID mapping is not unique. Setting different parallelism would lead to different mappings. The following
 * test only works when parallelism is 2.
 */
public class IDMappingUtilsTest extends AlinkTestBase {
	List <Row> originalData;
	List <Tuple2 <String, Long>> expectedIdMapping;
	List <Row> expectedMappedDataSet;
	List <long[]> randomWalks;
	List <String> expectedRemappedString;

	@Before
	public void before() {
		originalData = new ArrayList <>();
		originalData.add(Row.of("Alice", "Lisa", 1.));
		originalData.add(Row.of("Lisa", "Karry", 2.));
		originalData.add(Row.of("Karry", "Bella", 3.));
		originalData.add(Row.of("Bella", "Lucy", 4.));
		originalData.add(Row.of("Lucy", "Bob", 5.));
		originalData.add(Row.of("John", "Bob", 6.));
		originalData.add(Row.of("John", "Stella", 7.));
		originalData.add(Row.of("John", "Kate", 8.));
		originalData.add(Row.of("Kate", "Stella", 9.));
		originalData.add(Row.of("Kate", "Jack", 10.));
		originalData.add(Row.of("Jess", "Jack", 11.));

		expectedIdMapping = new ArrayList <>();
		expectedIdMapping.add(Tuple2.of("John", 7L));
		expectedIdMapping.add(Tuple2.of("Karry", 9L));
		expectedIdMapping.add(Tuple2.of("Lisa", 13L));
		expectedIdMapping.add(Tuple2.of("Bella", 0L));
		expectedIdMapping.add(Tuple2.of("Bob", 2L));
		expectedIdMapping.add(Tuple2.of("Stella", 4L));
		expectedIdMapping.add(Tuple2.of("Alice", 1L));
		expectedIdMapping.add(Tuple2.of("Jack", 3L));
		expectedIdMapping.add(Tuple2.of("Jess", 5L));
		expectedIdMapping.add(Tuple2.of("Kate", 11L));
		expectedIdMapping.add(Tuple2.of("Lucy", 15L));

		expectedMappedDataSet = new ArrayList <>();
		expectedMappedDataSet.add(Row.of(13L, 9L, 2.0));
		expectedMappedDataSet.add(Row.of(1L, 13L, 1.0));
		expectedMappedDataSet.add(Row.of(9L, 0L, 3.0));
		expectedMappedDataSet.add(Row.of(7L, 2L, 6.0));
		expectedMappedDataSet.add(Row.of(15L, 2L, 5.0));
		expectedMappedDataSet.add(Row.of(7L, 4L, 7.0));
		expectedMappedDataSet.add(Row.of(11L, 4L, 9.0));
		expectedMappedDataSet.add(Row.of(5L, 3L, 11.0));
		expectedMappedDataSet.add(Row.of(11L, 3L, 10.0));
		expectedMappedDataSet.add(Row.of(7L, 11L, 8.0));
		expectedMappedDataSet.add(Row.of(0L, 15L, 4.0));

		randomWalks = new ArrayList <>();
		randomWalks.add(new long[] {1, 13, 9, 0, 15});
		randomWalks.add(new long[] {15, 2, 7, 4});
		randomWalks.add(new long[] {7, 11, 3, 5});

		expectedRemappedString = new ArrayList <>();
		expectedRemappedString.add("Alice Lisa Karry Bella Lucy");
		expectedRemappedString.add("Lucy Bob John Stella");
		expectedRemappedString.add("John Kate Jack Jess");
	}

	@Test
	public void testComputeIdMapping() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet <Row> d = env.fromCollection(originalData);
		List <Tuple2 <String, Long>> result = IDMappingUtils.computeIdMapping(d, new int[] {0, 1}).collect();

		compareResultCollections(
			expectedIdMapping,
			result,
			new TestBaseUtils.TupleComparator <Tuple2 <String, Long>>());
	}

	@Test
	public void testMapDataSetWithIdMapping() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet <Row> d = env.fromCollection(originalData);
		DataSet <Tuple2 <String, Long>> idMapping = IDMappingUtils.computeIdMapping(d, new int[] {0, 1});
		List <Row> result = IDMappingUtils.mapDataSetWithIdMapping(d, idMapping, new int[] {0, 1}).collect();

		List <Tuple3 <Long, Long, Double>> mappedResult = result.stream().map(
			x -> Tuple3.of((Long) x.getField(0), (Long) x.getField(1), (Double) x.getField(2))).collect(
			Collectors.toList());

		List <Tuple3 <Long, Long, Double>> expectedMappedResult = expectedMappedDataSet.stream().map(
			x -> Tuple3.of((Long) x.getField(0), (Long) x.getField(1), (Double) x.getField(2))).collect(
			Collectors.toList());

		compareResultCollections(
			expectedMappedResult,
			mappedResult,
			new TupleComparator <Tuple3 <Long, Long, Double>>()
		);
	}

	@Test
	public void testRecoverDataSetWithIdMapping() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet <Row> d = env.fromCollection(originalData);
		DataSet <Tuple2 <String, Long>> idMapping = IDMappingUtils.computeIdMapping(d, new int[] {0, 1});
		DataSet <Row> mappedResult = IDMappingUtils.mapDataSetWithIdMapping(d, idMapping, new int[] {0, 1});
		List <Row> result = IDMappingUtils.recoverDataSetWithIdMapping(mappedResult, idMapping, new int[]{0, 1}).collect();

		List <Tuple3 <String, String, Double>> recoverResult = result.stream().map(
			x -> Tuple3.of((String) x.getField(0), (String) x.getField(1), (Double) x.getField(2))).collect(
			Collectors.toList());

		List <Tuple3 <String, String, Double>> expectedRecoveredResult = originalData.stream().map(
			x -> Tuple3.of((String) x.getField(0), (String) x.getField(1), (Double) x.getField(2))).collect(
			Collectors.toList());

		compareResultCollections(
			expectedRecoveredResult,
			recoverResult,
			new TupleComparator <Tuple3 <String, String, Double>>()
		);
	}

	@Test
	public void testMapWalkToStringWithIdMapping() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet <Row> d = env.fromCollection(originalData);
		DataSet <long[]> paths = env.fromCollection(randomWalks);
		DataSet <Tuple2 <String, Long>> idMapping = IDMappingUtils.computeIdMapping(d, new int[] {0, 1});
		List <Row> result = IDMappingUtils.mapWalkToStringWithIdMapping(paths, idMapping, 5, " ").collect();
		List <String> stringResult = result.stream().map(x -> (String) x.getField(0)).collect(Collectors.toList());
		compareResultCollections(expectedRemappedString, stringResult, new Comparator <String>() {
			@Override
			public int compare(String o1, String o2) {
				return o1.compareTo(o2);
			}
		});
	}
}
