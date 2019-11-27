package com.alibaba.alink.operator.batch.similarity;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Test for ApproxVectorSimilarityJoinLSHBatchOp.
 */
public class ApproxVectorSimilarityJoinLSHBatchOpTest {
	@Test
	public void testBRPSimilarity(){
		int len = 200, size = 10;
		Row[] left = new Row[len];
		Row[] right = new Row[len];
		for(int i = 0; i < left.length; i++){
			left[i] = Row.of(i, DenseVector.rand(size));
			right[i] = Row.of(i, DenseVector.rand(size));
		}

		MemSourceBatchOp leftData = new MemSourceBatchOp(Arrays.asList(left), new String[] {"leftId", "leftVec"});
		MemSourceBatchOp rightData = new MemSourceBatchOp(Arrays.asList(right), new String[] {"rightId", "rightVec"});

		BatchOperator op = new ApproxVectorSimilarityJoinLSHBatchOp()
			.setLeftCol("leftVec")
			.setRightCol("rightVec")
			.setLeftIdCol("leftId")
			.setRightIdCol("rightId")
			.setDistanceType("EUCLIDEAN")
			.setOutputCol("distance")
			.setDistanceThreshold(1.0);

		List<Row> res = op.linkFrom(leftData, rightData).collect();
		for(Row row : res){
			Assert.assertTrue((double)row.getField(2) < 1.0);
		}
	}

	@Test
	public void testMinSimilarity() {
		int len = 200, size = 10;
		Random random = new Random();
		Row[] left = new Row[len];
		Row[] right = new Row[len];
		for(int i = 0; i < left.length; i++){
			left[i] = Row.of(i, new SparseVector(size, new int[]{random.nextInt(size), random.nextInt(size)}, new double[]{random.nextDouble(), random.nextDouble()}));
			right[i] = Row.of(i, new SparseVector(size, new int[]{random.nextInt(size), random.nextInt(size)}, new double[]{random.nextDouble(), random.nextDouble()}));
		}

		MemSourceBatchOp leftData = new MemSourceBatchOp(Arrays.asList(left), new String[] {"leftId", "leftVec"});
		MemSourceBatchOp rightData = new MemSourceBatchOp(Arrays.asList(right), new String[] {"rightId", "rightVec"});

		BatchOperator op = new ApproxVectorSimilarityJoinLSHBatchOp()
			.setLeftCol("leftVec")
			.setRightCol("rightVec")
			.setLeftIdCol("leftId")
			.setRightIdCol("rightId")
			.setDistanceType("JACCARD")
			.setOutputCol("distance")
			.setDistanceThreshold(0.6);

		List<Row> res = op.linkFrom(leftData, rightData).collect();
		for(Row row : res){
			Assert.assertTrue((double)row.getField(2) < 0.6);
		}
	}
}
