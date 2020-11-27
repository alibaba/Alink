package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.JsonValueBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalMultiClassBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.linear.SoftmaxModelInfo;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class SoftmaxTest  extends AlinkTestBase {

	Row[] vecrows = new Row[] {
		Row.of("0:1.0 2:7.0 4:9.0", "1.0 7.0 9.0", 1.0, 7.0, 9.0, 2),
		Row.of("0:1.0 2:3.0 4:3.0", "1.0 3.0 3.0", 1.0, 3.0, 3.0, 3),
		Row.of("0:1.0 2:2.0 4:4.0", "1.0 2.0 4.0", 1.0, 2.0, 4.0, 1),
		Row.of("0:1.0 2:3.0 14:3.0", "1.0 3.0 3.0", 1.0, 3.0, 3.0, 4),
		Row.of("0:1.0 2:2.0 4:4.0", "1.0 2.0 4.0", 1.0, 2.0, 4.0, 5),
		Row.of("0:1.0 2:2.0 4:4.0", "1.0 2.0 4.0", 1.0, 2.0, 4.0, 6),
		Row.of("0:1.0 2:2.0 4:4.0", "1.0 2.0 4.0", 1.0, 2.0, 4.0, 7),
		Row.of("0:1.0 2:2.0 4:4.0", "1.0 2.0 4.0", 1.0, 2.0, 4.0, 1)
	};
	String[] veccolNames = new String[] {"svec", "vec", "f0", "f1", "f2", "label"};

	@Test
	public void batchTableTest() throws Exception {
		BatchOperator vecdata = new MemSourceBatchOp(Arrays.asList(vecrows), veccolNames);
		BatchOperator trainData = vecdata;
		String labelColName = "label";
		SoftmaxTrainBatchOp lr = new SoftmaxTrainBatchOp()
			.setVectorCol("svec")
			.setStandardization(true)
			.setWithIntercept(true)
			.setEpsilon(1.0e-4)
			.setL1(0.0001)
			.setOptimMethod("lbfgs")
			.setLabelCol(labelColName)
			.setMaxIter(20);

		SoftmaxTrainBatchOp model = lr.linkFrom(trainData);

		model.lazyPrintTrainInfo();
		model.lazyPrintModelInfo();
		model.lazyCollectModelInfo(new Consumer <SoftmaxModelInfo>() {
			@Override
			public void accept(SoftmaxModelInfo modelinfo) {
				String[] names = modelinfo.getFeatureNames();
				String vecName = modelinfo.getVectorColName();
				DenseVector[] dvs = modelinfo.getWeights();
				int size = modelinfo.getVectorSize();
				String modelName = modelinfo.getModelName();
				Object[] labelVals = modelinfo.getLabelValues();
				boolean isHas = modelinfo.hasInterceptItem();
			}
		});

		List <Row> acc = new SoftmaxPredictBatchOp()
			.setPredictionCol("predLr")
			.setPredictionDetailCol("predDetail")
			.linkFrom(model, trainData)
			.link(new EvalMultiClassBatchOp().setLabelCol("predLr").setPredictionDetailCol("predDetail"))
			.link(new JsonValueBatchOp()
				.setSelectedCol("Data")
				.setReservedCols(new String[] {"Statistics"})
				.setOutputCols(new String[] {"Accuracy"})
				.setJsonPath(new String[] {"$.Accuracy"}))
			.collect();
		Assert.assertEquals(Double.valueOf(acc.get(0).

			getField(0).

			toString()), 1.0, 0.001);
	}

	@Test
	public void batchVectorTest() throws Exception {
		BatchOperator vecdata = new MemSourceBatchOp(Arrays.asList(vecrows), veccolNames);
		BatchOperator trainData = vecdata;
		String labelColName = "label";
		SoftmaxTrainBatchOp lr = new SoftmaxTrainBatchOp()
			.setVectorCol("svec")
			.setStandardization(false)
			.setWithIntercept(true)
			.setEpsilon(1.0e-4)
			.setOptimMethod("LBFGS")
			.setLabelCol(labelColName)
			.setMaxIter(10);

		SoftmaxTrainBatchOp model = lr.linkFrom(trainData);

		model.lazyPrintTrainInfo();

		model.lazyPrintModelInfo();
		List <Row> acc = new SoftmaxPredictBatchOp()
			.setPredictionCol("predLr").setVectorCol("svec")
			.setPredictionDetailCol("predDetail")
			.linkFrom(model, trainData)
			.link(new EvalMultiClassBatchOp().setLabelCol("predLr").setPredictionDetailCol("predDetail"))
			.link(new JsonValueBatchOp()
				.setSelectedCol("Data")
				.setReservedCols(new String[] {"Statistics"})
				.setOutputCols(new String[] {"Accuracy"})
				.setJsonPath(new String[] {"$.Accuracy"}))
			.collect();
		Assert.assertEquals(Double.valueOf(acc.get(0).getField(0).toString()), 1.0, 0.001);
	}
}
