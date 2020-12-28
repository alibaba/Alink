package com.alibaba.alink.operator.batch.regression;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.fm.FmPredictBatchOp;
import com.alibaba.alink.operator.common.fm.FmRegressorModelInfo;
import com.alibaba.alink.pipeline.classification.FmModel;
import com.alibaba.alink.pipeline.regression.FmRegressor;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.function.Consumer;

public class FmRegressionTest extends AlinkTestBase {
	@Test
	public void testFm() throws Exception {
		AlinkGlobalConfiguration.setPrintProcessInfo(true);
		BatchOperator trainData = new MemSourceBatchOp(
			new Object[][] {
				{"1.1 2.0", 1.0},
				{"2.1 3.1", 1.0},
				{"3.1 2.2", 1.0},
				{"1.2 3.2", 0.0},
				{"1.2 4.2", 0.0}
			},
			new String[] {"vec", "label"});
		FmRegressorTrainBatchOp adagrad = new FmRegressorTrainBatchOp()
			.setVectorCol("vec")
			.setLabelCol("label")
			.setNumEpochs(10)
			.setInitStdev(0.01)
			.setLearnRate(0.01)
			.setEpsilon(0.0001)
			.linkFrom(trainData);
		adagrad.lazyPrintModelInfo();
		adagrad.lazyPrintTrainInfo();
		BatchOperator result = new FmPredictBatchOp().setVectorCol("vec").setPredictionCol("pred")
			.setPredictionDetailCol("details")
			.linkFrom(adagrad, trainData).print();
	}

	@Test
	public void testFmSparse() throws Exception {
		BatchOperator trainData = new MemSourceBatchOp(
			new Object[][] {
				{"1:1.1 3:2.0", 1.0},
				{"2:2.1 10:3.1", 1.0},
				{"3:3.1 7:2.2", 1.0},
				{"1:1.2 5:3.2", 0.0},
				{"3:1.2 7:4.2", 0.0}
			},
			new String[] {"vec", "label"});
		FmRegressorTrainBatchOp adagrad = new FmRegressorTrainBatchOp()
			.setVectorCol("vec")
			.setLabelCol("label")
			.setNumEpochs(10)
			.setInitStdev(0.01)
			.setLearnRate(0.01)
			.setEpsilon(0.0001)
			.linkFrom(trainData);

		adagrad.lazyCollectTrainInfo();
		adagrad.lazyPrintModelInfo();
		adagrad.lazyCollectModelInfo(new Consumer <FmRegressorModelInfo>() {
			@Override
			public void accept(FmRegressorModelInfo modelinfo) {
				String[] names = modelinfo.getFeatureColNames();
				String tast = modelinfo.getTask();
				double[][] factors = modelinfo.getFactors();
				int numFactor = modelinfo.getNumFactor();
				int size = modelinfo.getNumFeature();
			}
		});

		BatchOperator result = new FmPredictBatchOp().setVectorCol("vec").setPredictionCol("pred")
			.setPredictionDetailCol("details")
			.linkFrom(adagrad, trainData).print();
	}

	@Test
	public void testPipelineFmSparse() throws Exception {
		BatchOperator trainData = new MemSourceBatchOp(
			new Object[][] {
				{"0:1.1 1:2.0", 1.0},
				{"0:2.1 1:3.1", 1.0},
				{"0:3.1 1:2.2", 1.0},
				{"0:1.2 1:3.2", 0.0},
				{"0:1.2 1:4.2", 0.0}
			},
			new String[] {"vec", "label"});
		FmRegressor adagrad = new FmRegressor()
			.setVectorCol("vec")
			.setLabelCol("label")
			.setNumEpochs(10)
			.setInitStdev(0.01)
			.setLearnRate(0.01)
			.setEpsilon(0.0001)
			.setPredictionCol("pred")
			.enableLazyPrintModelInfo();

		FmModel model = adagrad.fit(trainData);
		BatchOperator result = model.transform(trainData);

		result.print();
	}
}
