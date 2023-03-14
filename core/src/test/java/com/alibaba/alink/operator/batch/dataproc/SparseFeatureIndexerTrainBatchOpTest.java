package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.params.dataproc.HasHandleDuplicateFeature.HandleDuplicate;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class SparseFeatureIndexerTrainBatchOpTest extends AlinkTestBase {
	/*
	 * fea3|4
	 * fea4|2
	 * fea5|1
	 * other2|1
	 * fea1|3
	 * fea2|3
	 * other1|1
	 */
	List <Row> dfData = Arrays.asList(
		Row.of("fea1:1,fea2:2,fea3:3,other2:1"),
		Row.of("fea2:1,fea3:4,fea4:1,other1:1"),
		Row.of("fea1:1,fea2:1,fea4:1,fea4:2"),
		Row.of("fea1:1,fea3:1,fea5:1")
	);
	BatchOperator <?> data = new MemSourceBatchOp(dfData, "feature string");

	@Test
	public void testTopN() throws Exception {
		int topn = 4;
		BatchOperator indexer = new SparseFeatureIndexerTrainBatchOp()
			.setSelectedCol("feature")
			.setTopN(topn)
			.setHasValue(true)
			.linkFrom(data);
		List<Row> result = indexer.collect();
		// first line is params meta
		Assert.assertEquals(result.size(), topn + 1);
		HashMap <Object, Object> map = new HashMap <>();
		for (Row r : result) {
			if (r.getField(1) != null) {
				map.put(r.getField(0), r.getField(1));
			}
		}
		Assert.assertEquals(map.size(), topn);
		Assert.assertNull(map.get("fea5"));
		Assert.assertNotNull(map.get("fea4"));
	}

	@Test
	public void testCandidateFunction() throws Exception {
		BatchOperator indexer = new SparseFeatureIndexerTrainBatchOp()
			.setSelectedCol("feature")
			.setHasValue(true)
			.setCandidateTags("fea")
			.linkFrom(data);
		List<Row> result = indexer.collect();
		HashMap <Object, Object> map = new HashMap <>();
		for (Row r : result) {
			if (r.getField(1) != null) {
				map.put(r.getField(0), r.getField(1));
			}
		}
		Assert.assertEquals(map.size(), 5);
		Assert.assertNull(map.get("other1"));
		Assert.assertNotNull(map.get("fea5"));
	}

	@Test
	public void testMinFrequency() throws Exception {
		BatchOperator indexer = new SparseFeatureIndexerTrainBatchOp()
			.setSelectedCol("feature")
			.setHasValue(true)
			.setMinFrequency(2)
			.linkFrom(data);
		List<Row> result = indexer.collect();
		HashMap <Object, Object> map = new HashMap <>();
		for (Row r : result) {
			if (r.getField(1) != null) {
				map.put(r.getField(0), r.getField(1));
			}
		}
		Assert.assertEquals(map.size(), 4);
		Assert.assertNull(map.get("other1"));
		Assert.assertNotNull(map.get("fea4"));
	}

	@Test
	public void testMinPercent() throws Exception {
		BatchOperator indexer = new SparseFeatureIndexerTrainBatchOp()
			.setSelectedCol("feature")
			.setHasValue(true)
			.setMinPercent(0.5)
			.linkFrom(data);
		List<Row> result = indexer.collect();
		HashMap <Object, Object> map = new HashMap <>();
		for (Row r : result) {
			if (r.getField(1) != null) {
				map.put(r.getField(0), r.getField(1));
			}
		}
		Assert.assertEquals(map.size(), 4);
		Assert.assertNull(map.get("fea5"));
		Assert.assertNotNull(map.get("fea4"));
	}

	@Test
	public void testPredictLast() throws Exception {
		List <Row> predictData = Arrays.asList(
			Row.of("fea1:1,fea2:1,fea1:2")
		);
		BatchOperator <?> predictBatch = new MemSourceBatchOp(predictData, "feature string");
		BatchOperator indexer = new SparseFeatureIndexerTrainBatchOp()
			.setSelectedCol("feature")
			.setHasValue(true)
			.setMinPercent(0.5)
			.linkFrom(data);
		BatchOperator result = new SparseFeatureIndexerPredictBatchOp()
			.setSelectedCol("feature")
			.setOutputCol("vec")
			.setHandleDuplicate(HandleDuplicate.LAST)
			.linkFrom(indexer, predictBatch);
		List<Row> list = result.collect();
		SparseVector sv = (SparseVector) list.get(0).getField(1);
		Assert.assertEquals(sv.get(0), 2.0, 0.0000001);
	}

	@Test
	public void testPredictFirst() throws Exception {
		List <Row> predictData = Arrays.asList(
			Row.of("fea1:1,fea2:1,fea1:2")
		);
		BatchOperator <?> predictBatch = new MemSourceBatchOp(predictData, "feature string");
		BatchOperator indexer = new SparseFeatureIndexerTrainBatchOp()
			.setSelectedCol("feature")
			.setHasValue(true)
			.setMinPercent(0.5)
			.linkFrom(data);
		BatchOperator result = new SparseFeatureIndexerPredictBatchOp()
			.setSelectedCol("feature")
			.setOutputCol("vec")
			.setHandleDuplicate(HandleDuplicate.FIRST)
			.linkFrom(indexer, predictBatch);
		List<Row> list = result.collect();
		SparseVector sv = (SparseVector) list.get(0).getField(1);
		Assert.assertEquals(sv.get(0), 1.0, 0.0000001);
	}
}
