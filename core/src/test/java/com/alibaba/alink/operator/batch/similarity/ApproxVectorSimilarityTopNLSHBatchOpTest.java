package com.alibaba.alink.operator.batch.similarity;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * Test for ApproxVectorSimilarityTopNLSHBatchOp.
 */
public class ApproxVectorSimilarityTopNLSHBatchOpTest {
	@Test
	public void testBRPSimilarity(){
		int len = 200, size = 10;
		Row[] left = new Row[len];
		for(int i = 0; i < left.length; i++){
			left[i] = Row.of(i, DenseVector.rand(size));
		}

		MemSourceBatchOp leftData = new MemSourceBatchOp(Arrays.asList(left), new String[] {"leftId", "leftVec"});

		BatchOperator op = new ApproxVectorSimilarityTopNLSHBatchOp()
			.setLeftCol("leftVec")
			.setRightCol("leftVec")
			.setLeftIdCol("leftId")
			.setRightIdCol("leftId")
			.setDistanceType("EUCLIDEAN")
			.setOutputCol("distance");

		List<Row> res = op.linkFrom(leftData, leftData).collect();
		Assert.assertTrue(res.size() >= 200 && res.size() <= 1000);
	}

	@Test
	public void testMinSimilarity() {
		int len = 200, size = 10;
		Row[] left = new Row[len];
		Random random = new Random();
		for(int i = 0; i < left.length; i++){
			left[i] = Row.of(i, new SparseVector(size, new int[]{random.nextInt(size), random.nextInt(size)}, new double[]{random.nextDouble(), random.nextDouble()}));
		}

		MemSourceBatchOp leftData = new MemSourceBatchOp(Arrays.asList(left), new String[] {"leftId", "leftVec"});

		BatchOperator op = new ApproxVectorSimilarityTopNLSHBatchOp()
			.setLeftCol("leftVec")
			.setRightCol("leftVec")
			.setLeftIdCol("leftId")
			.setRightIdCol("leftId")
			.setDistanceType("JACCARD")
			.setOutputCol("distance");

		List<Row> res = op.linkFrom(leftData, leftData).collect();
		Assert.assertTrue(res.size() >= 200 && res.size() <= 1000);
	}
}
