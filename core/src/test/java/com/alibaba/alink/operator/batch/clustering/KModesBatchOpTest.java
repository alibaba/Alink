package com.alibaba.alink.operator.batch.clustering;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.clustering.KModesPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class KModesBatchOpTest extends AlinkTestBase {

	private MemSourceBatchOp inputTableBatch;
	private MemSourceBatchOp predictTableBatch;
	private MemSourceStreamOp predictTableStream;
	private String[] featureColNames;

	private int k = 10;

	@Before
	public void setUp() throws Exception {
		genTable();
	}

	void genTable() {
		//data
		int row = 100;
		int col = 10;
		Random random = new Random(2018L);

		String[] alphaTable = new String[] {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m"};

		Object[][] objs = new Object[row][col + 1];
		String[] colNames = new String[col + 1];
		featureColNames = new String[col];
		colNames[0] = "id";

		for (int i = 0; i < col; i++) {
			colNames[i + 1] = "col" + i;
			featureColNames[i] = colNames[i + 1];
		}

		for (int i = 0; i < row; i++) {
			objs[i][0] = (long) i;
			for (int j = 0; j < col; j++) {
				objs[i][j + 1] = alphaTable[random.nextInt(12)];
			}
		}

		inputTableBatch = new MemSourceBatchOp(objs, colNames);
		predictTableBatch = new MemSourceBatchOp(objs, colNames);
		predictTableStream = new MemSourceStreamOp(objs, colNames);

	}

	@Test
	public void testTable() throws Exception {
		// kModes batch
		KModesTrainBatchOp kModesBatchOp = new KModesTrainBatchOp()
			.setK(k)
			.setFeatureCols(featureColNames)
			.setNumIter(10)
			.linkFrom(inputTableBatch);

		// kModes batch predict
		KModesPredictBatchOp kModesPredictBatchOp = new KModesPredictBatchOp()
			.setReservedCols(new String[] {"id"})
			.setPredictionCol("cluster_id")
			.linkFrom(kModesBatchOp, predictTableBatch);

		int modelLineNumber = kModesBatchOp.collect().size();
		Assert.assertArrayEquals(new int[] {modelLineNumber}, new int[] {k + 1});
		Assert.assertEquals(kModesPredictBatchOp.select("cluster_id").distinct().count(), k);

		// kModes stream predict
		KModesPredictStreamOp kModesPredictStreamOp = new KModesPredictStreamOp(kModesBatchOp)
			.setReservedCols(new String[] {"id"})
			.setPredictionCol("cluster_id").setPredictionDetailCol("cluster_detail");
		kModesPredictStreamOp.linkFrom(predictTableStream).print();
		StreamOperator.execute();

	}

}
