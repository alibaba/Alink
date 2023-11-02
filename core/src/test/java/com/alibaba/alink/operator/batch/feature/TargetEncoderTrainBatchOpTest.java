package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;

public class TargetEncoderTrainBatchOpTest extends AlinkTestBase {
	Row[] array = new Row[] {
		Row.of(1., "a", 1., 1., 1., 2, "l1"),
		Row.of(1., "a", 1., 0., 1., 2, "l1"),
		Row.of(1., "b", 0., 1., 1., 3, "l1"),
		Row.of(1., "d", 0., 1., 1.5, 2, "l1"),
		Row.of(2., "c", 1.5, 1., 0.5, 3, "l0"),
		Row.of(1., "a", 1., 1.5, 0., 1, "l0"),
		Row.of(2., "d", 1., 1., 0., 1, "l0"),
	};


	@Test
	public void testTrain() throws Exception {
		BatchOperator data = new MemSourceBatchOp(
			Arrays.asList(array), new String[] {"weight", "f0", "f1", "f2", "f3", "f4", "label"});

		TargetEncoderTrainBatchOp train = new TargetEncoderTrainBatchOp()
			.setPositiveLabelValueString("l1")
			.setLabelCol("label")
			.setSelectedCols("f0", "f4");
		train.linkFrom(data).print();

		TargetEncoderPredictBatchOp pred = new TargetEncoderPredictBatchOp()
			.setOutputCols("f0_suffix", "f4_suffix")
			.linkFrom(train, data);

		pred.print();
	}


}