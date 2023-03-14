package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class GroupKMeansBatchOpTest extends AlinkTestBase {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private MemSourceBatchOp inputOp;

	private List<Row> trainData = Arrays.asList(
		Row.of(0, "id_1", 2.0, 3.0),
		Row.of(0, "id_2", 2.1, 3.1),
		Row.of(0, "id_18", 2.4, 3.2),
		Row.of(0, "id_15", 2.8, 3.2),
		Row.of(0, "id_12", 2.1, 3.1),
		Row.of(0, "id_3", 200.1, 300.1),
		Row.of(0, "id_4", 200.2, 300.2),
		Row.of(0, "id_8", 200.6, 300.6),

		Row.of(1, "id_5", 200.3, 300.3),
		Row.of(1, "id_6", 200.4, 300.4),
		Row.of(1, "id_7", 200.5, 300.5),
		Row.of(1, "id_16", 300., 300.2),
		Row.of(1, "id_9", 2.1, 3.1),
		Row.of(1, "id_10", 2.2, 3.2),
		Row.of(1, "id_11", 2.3, 3.3),
		Row.of(1, "id_13", 2.4, 3.4),
		Row.of(1, "id_14", 2.5, 3.5),
		Row.of(1, "id_17", 2.6, 3.6),
		Row.of(1, "id_19", 2.7, 3.7),
		Row.of(1, "id_20", 2.8, 3.8),
		Row.of(1, "id_21", 2.9, 3.9),

		Row.of(2, "id_20", 2.8, 3.8)
	);

	List <String[]> expectedClustering = new ArrayList <>(5);
	Map<String, Long> sizeByGroupName = new HashMap <>();

	@Before
	public void before() {
		expectedClustering.add(new String[] {"id_1", "id_2", "id_12", "id_15", "id_18"});
		expectedClustering.add(new String[] {"id_3", "id_4", "id_8"});
		expectedClustering.add(
			new String[] {"id_9", "id_10", "id_11", "id_13", "id_14", "id_17", "id_19", "id_20", "id_21"});
		expectedClustering.add(new String[] {"id_5", "id_6", "id_7", "id_16"});
		expectedClustering.add(new String[] {"id_20"});
		for (String[] group : expectedClustering) {
			Arrays.sort(group);
		}
		Collections.shuffle(trainData);
		inputOp = new MemSourceBatchOp(trainData,
			new String[] {"group", "id", "c1", "c2"});

		sizeByGroupName.put("group1", 100L);
		sizeByGroupName.put("group2", 1L);
		sizeByGroupName.put("group3", 2L);
		sizeByGroupName.put("group4", 3L);
		sizeByGroupName.put("group5", 300L);
	}

	/** Verifies the partition info. */
	private void verifyPartitionInfo(Map <String, int[]> groupNameAndOwnerWorker, Map<String, Long> sizeByGroupName, int numWorkers) {
		long[] elementsHandledEachWorker = new long[numWorkers];
		for (Map.Entry<String, int[]> groupNameAndOwner: groupNameAndOwnerWorker.entrySet()) {
			String groupName = groupNameAndOwner.getKey();
			int[]workerIds = groupNameAndOwner.getValue();
			long numElements = sizeByGroupName.get(groupName);
			for (int workerId: workerIds) {
				elementsHandledEachWorker[workerId] += numElements / workerIds.length;
			}
		}

		long averageNumElementsPerWorker = 0;
		for (long numElements: sizeByGroupName.values()) {
			averageNumElementsPerWorker += numElements;
		}
		averageNumElementsPerWorker /= numWorkers;
		for (long handledElements: elementsHandledEachWorker) {
			assertTrue(handledElements >= 0);
			assertTrue(handledElements <= 2 * averageNumElementsPerWorker);
		}
	}

	@Test
	public void testGetPartitionInfo() {
		int numWorkers = 3;
		Map <String, int[]> partitionInfo =
			GroupKMeansBatchOp.getPartitionInfo(sizeByGroupName, numWorkers);
		verifyPartitionInfo(partitionInfo, sizeByGroupName, numWorkers);
	}

	/**
	 * Verifies that nodes with same groups and predictions must be in the same group.
	 *
	 * @param prediction row: (groupId, nodeIdx, prediction)
	 */
	private void verifyPredictionResult(List <Row> prediction) {
		// (group, pred) -> nodeIds in this group with the prediction.
		HashMap <Tuple2 <Integer, Long>, HashSet <String>> groupedNodeIdsByGroupIdAndPrediction = new HashMap <>();
		for (Row r : prediction) {
			Tuple2 <Integer, Long> key = Tuple2.of((Integer) r.getField(0), (Long) r.getField(2));
			groupedNodeIdsByGroupIdAndPrediction.putIfAbsent(key, new HashSet <>());
			groupedNodeIdsByGroupIdAndPrediction.get(key).add((String) r.getField(1));
		}
		List <String[]> actualGrouping = new ArrayList <>();
		for (HashSet <String> group : groupedNodeIdsByGroupIdAndPrediction.values()) {
			String[] groupArray = group.toArray(new String[0]);
			Arrays.sort(groupArray);
			actualGrouping.add(groupArray);
		}

		assertEquals(expectedClustering.size(), actualGrouping.size());
		Comparator <String[]> stringArrayComparator = Comparator.comparing(Arrays::toString);
		expectedClustering.sort(stringArrayComparator);
		actualGrouping.sort(stringArrayComparator);
		for (int i = 0; i < expectedClustering.size(); ++i) {
			assertArrayEquals(expectedClustering.get(i), actualGrouping.get(i));
		}
	}

	@Test
	public void testGroupKmeans() {
		GroupKMeansBatchOp op = new GroupKMeansBatchOp()
			.setGroupCols(new String[] {"group"})
			.setK(2)
			.setMaxIter(50)
			.setPredictionCol("pred")
			.setEpsilon(1e-8)
			.setFeatureCols(new String[] {"c1", "c2"})
			.setIdCol("id")
			.linkFrom(inputOp);

		assertEquals(22, op.collect().size());
	}

	@Test
	public void testFeatureColsNotSet() {
		thrown.expect(RuntimeException.class);
		thrown.expectMessage("featureColNames should be set !");
		GroupKMeansBatchOp op = new GroupKMeansBatchOp()
			.setFeatureCols()
			.setIdCol("id")
			.setGroupCols("group")
			.setPredictionCol("pred")
			.setEpsilon(0.6)
			.linkFrom(inputOp);
		op.collect();
	}

	@Test
	public void testIdColInFeatureCols() {
		thrown.expect(RuntimeException.class);
		thrown.expectMessage("idCol column should NOT be included in featureColNames !");
		GroupKMeansBatchOp op = new GroupKMeansBatchOp()
			.setFeatureCols("c1", "c2")
			.setIdCol("c1")
			.setGroupCols("group")
			.setEpsilon(0.6)
			.setPredictionCol("pred")
			.linkFrom(inputOp);
		op.collect();
	}

	@Test
	public void testIdColInGroupCols() {
		thrown.expect(RuntimeException.class);
		thrown.expectMessage("idCol column should NOT be included in groupColNames ");
		GroupKMeansBatchOp op = new GroupKMeansBatchOp()
			.setFeatureCols("c1", "c2")
			.setIdCol("group")
			.setGroupCols("group")
			.setEpsilon(0.6)
			.setPredictionCol("pred")
			.linkFrom(inputOp);
		op.collect();
	}

	@Test
	public void testGroupColsInFeatureCols() {
		thrown.expect(RuntimeException.class);
		thrown.expectMessage("groupColNames should NOT be included in featureColNames!");
		GroupKMeansBatchOp op = new GroupKMeansBatchOp()
			.setFeatureCols("group", "c1", "c2")
			.setIdCol("id")
			.setGroupCols("group")
			.setPredictionCol("pred")
			.setEpsilon(0.6)
			.linkFrom(inputOp);
		op.collect();
	}


	@Test
	public void testHighDimensionData() {
		int dim = 1000;
		String[] featureCols = getHighDimFeatureNames(dim);
		Row data = getHighDimRows(dim);
		MemSourceBatchOp inputOp = new MemSourceBatchOp(Collections.singletonList(data),
			ArrayUtils.addAll(featureCols, "groupCol", "idCol" ));
		GroupKMeansBatchOp op = new GroupKMeansBatchOp()
			.setFeatureCols(featureCols)
			.setIdCol("idCol")
			.setGroupCols("groupCol")
			.setPredictionCol("pred")
			.setEpsilon(0.6)
			.linkFrom(inputOp);
		op.collect();
	}

	private Row getHighDimRows(int dim) {
		Row row = new Row(dim + 2);
		row.setField(dim, "group1");
		row.setField(dim + 1, "id1");
		for (int i = 0; i < dim; i ++) {
			row.setField(i, 1.0);
		}
		return row;
	}

	private String[] getHighDimFeatureNames(int dim) {
		String[] features = new String[dim];
		for (int i = 0; i < dim; i ++) {
			features[i] = "f" + i;
		}
		return features;
	}
}