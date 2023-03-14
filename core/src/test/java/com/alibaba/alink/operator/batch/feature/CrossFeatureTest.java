package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.feature.CrossFeaturePredictLocalOp;
import com.alibaba.alink.operator.local.feature.CrossFeatureTrainLocalOp;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.HashCrossFeatureStreamOp;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class CrossFeatureTest extends AlinkTestBase {

	Row[] array = new Row[] {
		Row.of(1, "1.0", "1.0", 1.0, 1),
		Row.of(2, "1.0", "1.0", 0.0, 1),
		Row.of(3, "1.0", "0.0", 1.0, 1),
		Row.of(4, "1.0", "0.0", 1.0, 1),
		Row.of(5, "2.0", "3.0", null, 0),
		Row.of(6, "2.0", "3.0", 1.0, 0),
		Row.of(7, "0.0", "1.0", 2.0, 0),
		Row.of(8, "0.0", "1.0", 1.0, 0)
	};
	String[] vecColNames = new String[] {"id", "f0", "f1", "f2", "label"};

	@Test
	public void testCross() throws Exception {
		List <Row> expected = Arrays.asList(
			Row.of(1, "1.0", "1.0", 1.0000, 1, VectorUtil.getVector("$36$0:1.0")),
			Row.of(2, "1.0", "1.0", 0.0000, 1, VectorUtil.getVector("$36$9:1.0")),
			Row.of(3, "1.0", "0.0", 1.0000, 1, VectorUtil.getVector("$36$6:1.0")),
			Row.of(4, "1.0", "0.0", 1.0000, 1, VectorUtil.getVector("$36$6:1.0")),
			Row.of(5, "2.0", "3.0", null, 0, VectorUtil.getVector("$36$22:1.0")),
			Row.of(6, "2.0", "3.0", 1.0000, 0, VectorUtil.getVector("$36$4:1.0")),
			Row.of(7, "0.0", "1.0", 2.0000, 0, VectorUtil.getVector("$36$29:1.0")),
			Row.of(8, "0.0", "1.0", 1.0000, 0, VectorUtil.getVector("$36$2:1.0"))
		);

		BatchOperator <?> data = new MemSourceBatchOp(array, vecColNames);

		CrossFeatureTrainBatchOp train = new CrossFeatureTrainBatchOp()
			.setSelectedCols("f0", "f1", "f2")
			.linkFrom(data);

		train.lazyPrint(-1, "model");
		train.getSideOutput(0).lazyPrint(-1, "side output");

		CrossFeaturePredictBatchOp pred = new CrossFeaturePredictBatchOp()
			.setOutputCol("cross")
			.linkFrom(train, data);

		assertListRowEqual(expected, pred.collect(), 0);
	}

	@Test
	public void testCrossLocal() throws Exception {
		List <Row> expected = Arrays.asList(
			Row.of(1, "1.0", "1.0", 1.0000, 1, VectorUtil.getVector("$36$0:1.0")),
			Row.of(2, "1.0", "1.0", 0.0000, 1, VectorUtil.getVector("$36$9:1.0")),
			Row.of(3, "1.0", "0.0", 1.0000, 1, VectorUtil.getVector("$36$6:1.0")),
			Row.of(4, "1.0", "0.0", 1.0000, 1, VectorUtil.getVector("$36$6:1.0")),
			Row.of(5, "2.0", "3.0", null, 0, VectorUtil.getVector("$36$22:1.0")),
			Row.of(6, "2.0", "3.0", 1.0000, 0, VectorUtil.getVector("$36$4:1.0")),
			Row.of(7, "0.0", "1.0", 2.0000, 0, VectorUtil.getVector("$36$29:1.0")),
			Row.of(8, "0.0", "1.0", 1.0000, 0, VectorUtil.getVector("$36$2:1.0"))
		);

		LocalOperator <?> data = new MemSourceLocalOp(array, vecColNames);

		CrossFeatureTrainLocalOp train = new CrossFeatureTrainLocalOp()
			.setSelectedCols("f0", "f1", "f2")
			.linkFrom(data);

		train.lazyPrint(-1, "model");
		//train.getSideOutput(0).lazyPrint(-1, "side output");

		CrossFeaturePredictLocalOp pred = new CrossFeaturePredictLocalOp()
			.setOutputCol("cross")
			.linkFrom(train, data)
			.print();
	}

	@Test
	public void testHash() throws Exception {
		List <Row> expected = Arrays.asList(
			Row.of(1, "1.0", "1.0", 1.0, 1, VectorUtil.getVector("$36$21:1.0")),
			Row.of(2, "1.0", "1.0", 0.0, 1, VectorUtil.getVector("$36$3:1.0")),
			Row.of(3, "1.0", "0.0", 1.0, 1, VectorUtil.getVector("$36$33:1.0")),
			Row.of(4, "1.0", "0.0", 1.0, 1, VectorUtil.getVector("$36$33:1.0")),
			Row.of(5, "2.0", "3.0", null, 0, VectorUtil.getVector("$36$")),
			Row.of(6, "2.0", "3.0", 1.0, 0, VectorUtil.getVector("$36$16:1.0")),
			Row.of(7, "0.0", "1.0", 2.0, 0, VectorUtil.getVector("$36$3:1.0")),
			Row.of(8, "0.0", "1.0", 1.0, 0, VectorUtil.getVector("$36$8:1.0"))
		);

		StreamOperator <?> data = new MemSourceStreamOp(Arrays.asList(array), vecColNames);
		HashCrossFeatureStreamOp hashCross = new HashCrossFeatureStreamOp()
			.setNumFeatures(36)
			.setSelectedCols("f0", "f1", "f2")
			.setOutputCol("res")
			.linkFrom(data);
		CollectSinkStreamOp sink = new CollectSinkStreamOp()
			.linkFrom(hashCross);
		StreamOperator.execute();

		assertListRowEqual(expected, sink.getAndRemoveValues(), 0);
	}
}
