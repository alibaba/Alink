package com.alibaba.alink.pipeline.regression;

import java.util.Arrays;
import java.util.List;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.AftSurvivalRegPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.AftSurvivalRegTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for AFTRegression.
 */
public class AFTRegTest {
	private Row[] rows = new Row[] {
		Row.of(1.218, 1.0, "1.560,-0.605"),
		Row.of(2.949, 0.0, "0.346,2.158"),
		Row.of(3.627, 0.0, "1.380,0.231"),
		Row.of(0.273, 1.0, "0.520,1.151"),
		Row.of(4.199, 0.0, "0.795,-0.226")
	};

	private Row[] rowsSparse = new Row[] {
			Row.of(1.218, 1.0, "$10$3:1.560,7:-0.605"),
			Row.of(2.949, 0.0, "$10$3:0.346,7:2.158"),
			Row.of(3.627, 0.0, "$10$3:1.380,7:0.231"),
			Row.of(0.273, 1.0, "$10$3:0.520,7:1.151"),
			Row.of(4.199, 0.0, "$10$3:0.795,7:-0.226")
	};

	private Row[] rowsFeatures = new Row[] {
			Row.of(1.218, 1.0, 1.560, -0.605),
			Row.of(2.949, 0.0, 0.346, 2.158),
			Row.of(3.627, 0.0, 1.380, 0.231),
			Row.of(0.273, 1.0, 0.520, 1.151),
			Row.of(4.199, 0.0, 0.795, -0.226),
	};


	private MemSourceBatchOp dataSparse = new MemSourceBatchOp(Arrays.asList(rowsSparse), new String[] {"label", "censor", "features"});
	private MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(rows), new String[] {"label", "censor", "features"});
	private MemSourceBatchOp dataFeatures = new MemSourceBatchOp(Arrays.asList(rowsFeatures), new String[] {"label", "censor", "f0", "f1"});

	@Test
	public void testPipeline() throws Exception {
		AftSurvivalRegression reg = new AftSurvivalRegression()
			.setVectorCol("features")
			.setLabelCol("label")
			.setCensorCol("censor")
			.setPredictionCol("result");
		PipelineModel model = new Pipeline().add(reg).fit(data);
		Table res = model.transform(data.getOutputTable());
		List <Double> list = MLEnvironmentFactory.getDefault()
				.getBatchTableEnvironment().toDataSet(res.select("result"), Double.class)
			.collect();
		double[] actual = new double[] {5.70, 18.10, 7.36, 13.62, 9.03};
		for (int i = 0; i < actual.length; i++) {
			Assert.assertEquals(list.get(i), actual[i], 0.1);
		}
	}

	@Test
	public void testBatchSparseWithoutIntercept() throws Exception {
		AftSurvivalRegTrainBatchOp reg = new AftSurvivalRegTrainBatchOp()
				.setVectorCol("features")
					.setLabelCol("label")
				.setWithIntercept(false)
				.setCensorCol("censor");
		BatchOperator res = new AftSurvivalRegPredictBatchOp().setPredictionCol("pred")
				.linkFrom(reg.linkFrom(dataSparse), dataSparse);
		List<Row> list = res.select("pred").getDataSet().collect();
		double[] actual = new double[]{10.05, 19.26, 17.45, 9.14, 3.54};
		for (int i = 0; i < actual.length; i++) {
			Assert.assertEquals((double) list.get(i).getField(0), actual[i], 0.1);
		}
	}

	@Test
	public void testBatchDenseWithoutIntercept() throws Exception {
		AftSurvivalRegTrainBatchOp reg = new AftSurvivalRegTrainBatchOp()
				.setVectorCol("features")
				.setLabelCol("label")
				.setWithIntercept(false)
				.setCensorCol("censor");
		BatchOperator res = new AftSurvivalRegPredictBatchOp().setPredictionCol("pred")
				.linkFrom(reg.linkFrom(data), data);
		List<Row> list = res.select("pred").getDataSet().collect();
		double[] actual = new double[]{10.05, 19.26, 17.45, 9.14, 3.54};
		for (int i = 0; i < actual.length; i++) {
			Assert.assertEquals((double) list.get(i).getField(0), actual[i], 0.1);
		}
	}


	@Test
	public void testBatchDenseWithIntercept() throws Exception {
		AftSurvivalRegTrainBatchOp reg = new AftSurvivalRegTrainBatchOp()
				.setVectorCol("features")
				.setLabelCol("label")
				.setCensorCol("censor");
		BatchOperator res = new AftSurvivalRegPredictBatchOp().setPredictionCol("pred")
				.linkFrom(reg.linkFrom(data), data);
		List<Row> list = res.select("pred").getDataSet().collect();
		double[] actual = new double[]{5.70, 18.10, 7.36, 13.62, 9.03};
		for (int i = 0; i < actual.length; i++) {
			Assert.assertEquals((double) list.get(i).getField(0), actual[i], 0.1);
		}
	}

	@Test
	public void testFeatures() throws Exception {
		AftSurvivalRegTrainBatchOp reg = new AftSurvivalRegTrainBatchOp()
				.setFeatureCols("f0", "f1")
				.setLabelCol("label")
				.setWithIntercept(false)
				.setCensorCol("censor");
		BatchOperator model = reg.linkFrom(dataFeatures);
		model.print();
		BatchOperator res = new AftSurvivalRegPredictBatchOp().setPredictionCol("pred")
				.linkFrom(model, dataFeatures);
		List<Row> list = res.select("pred").getDataSet().collect();
		double[] actual = new double[]{10.05, 19.26, 17.45, 9.14, 3.54};
		for (int i = 0; i < actual.length; i++) {
			Assert.assertEquals((double) list.get(i).getField(0), actual[i], 0.1);
		}
	}
}
