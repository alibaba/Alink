package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.HashCrossFeatureStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class CrossFeatureTest extends AlinkTestBase {

	Row[] array = new Row[] {
		Row.of(new Object[] {"1.0", "1.0", 1.0, 1}),
		Row.of(new Object[] {"1.0", "1.0", 0.0, 1}),
		Row.of(new Object[] {"1.0", "0.0", 1.0, 1}),
		Row.of(new Object[] {"1.0", "0.0", 1.0, 1}),
		Row.of(new Object[] {"2.0", "3.0", null, 0}),
		Row.of(new Object[] {"2.0", "3.0", 1.0, 0}),
		Row.of(new Object[] {"0.0", "1.0", 2.0, 0}),
		Row.of(new Object[] {"0.0", "1.0", 1.0, 0})
	};
	String[] vecColNames = new String[] {"f0", "f1", "f2", "label"};

	Row[] predArray = new Row[] {
		Row.of(new Object[] {"1.0", "1.0", 3.0, 1}),
		Row.of(new Object[] {"1.0", "0.0", 0.0, 1}),
		Row.of(new Object[] {"1.0", "0.0", 1.0, 1}),
		Row.of(new Object[] {null, "0.0", 1.0, 1}),
		Row.of(new Object[] {"2.0", "3.0", null, 0}),
		Row.of(new Object[] {"2.0", "3.0", 1.0, 0}),
		Row.of(new Object[] {"1.0", "2.0", 2.0, 0}),
		Row.of(new Object[] {"0.0", "1.0", 3.0, 0})
	};
	String[] predVecColNames = new String[] {"f0", "f1", "f2", "label"};

	@Test
	public void testCross() throws Exception {
		BatchOperator data = new MemSourceBatchOp(array, vecColNames);
		String[] selectedCols = new String[] {"f0", "f1", "f2"};
		CrossFeatureTrainBatchOp train = new CrossFeatureTrainBatchOp()
			.setSelectedCols(selectedCols)
			.linkFrom(data);

		train.lazyPrint(-1, "model");
		train.getSideOutput(0).lazyPrint(-1, "side output");

		BatchOperator predData = new MemSourceBatchOp(predArray, predVecColNames);
		CrossFeaturePredictBatchOp pred = new CrossFeaturePredictBatchOp()
			.setOutputCol("cross")
			.linkFrom(train, predData);
		pred.lazyPrint(-1);
		BatchOperator.execute();
		//Assert.assertEquals(pred.collect().size(), 8);
	}

	@Test
	public void testHash() throws Exception {
		StreamOperator data = new MemSourceStreamOp(Arrays.asList(array), vecColNames);
		HashCrossFeatureStreamOp hashCross = new HashCrossFeatureStreamOp()
			.setNumFeatures(4)
			.setSelectedCols("f0", "f1", "f2")
			.setOutputCol("res")
			.linkFrom(data);
		hashCross.print();
		StreamOperator.execute();
	}

}
