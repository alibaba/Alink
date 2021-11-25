package com.alibaba.alink.pipeline.regression;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.AftSurvivalRegPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.AftSurvivalRegTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Test for AFTRegression.
 */

public class AFTRegTest extends AlinkTestBase {
	private static final Row[] rows = new Row[] {
		Row.of(0, 1.218, 1.0, "1.560,-0.605"),
		Row.of(1, 2.949, 0.0, "0.346,2.158"),
		Row.of(2, 3.627, 0.0, "1.380,0.231"),
		Row.of(3, 0.273, 1.0, "0.520,1.151"),
		Row.of(4, 4.199, 0.0, "0.795,-0.226")
	};

	@Test
	public void testPipeline() {
		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(rows),
			new String[] {"id", "label", "censor", "features"});

		AftSurvivalRegression reg = new AftSurvivalRegression()
			.setVectorCol("features")
			.setLabelCol("label")
			.setCensorCol("censor")
			.setPredictionCol("result").enableLazyPrintModelInfo().enableLazyPrintTrainInfo();
		PipelineModel model = new Pipeline().add(reg).fit(data);
		BatchOperator <?> res = model.transform(data);
		List <Row> list = res.select(new String[] {"id", "result"}).collect();
		double[] actual = new double[] {5.70, 18.10, 7.36, 13.62, 9.03};
		for (int i = 0; i < actual.length; i++) {
			Assert.assertEquals((Double) list.get(i).getField(1), actual[(int) list.get(i).getField(0)], 0.1);
		}
	}

	@Test
	public void testOp() throws Exception {
		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(rows),
			new String[] {"id", "label", "censor", "features"});
		AftSurvivalRegTrainBatchOp trainBatchOp = new AftSurvivalRegTrainBatchOp()
			.setVectorCol("features")
			.setLabelCol("label")
			.setCensorCol("censor").linkFrom(data);

		BatchOperator <?> res = new AftSurvivalRegPredictBatchOp()
			.setPredictionCol("result")
			.setPredictionDetailCol("detail").linkFrom(trainBatchOp, data);
		List <Row> list = res.select(new String[] {"id", "result"}).collect();
		double[] actual = new double[] {5.70, 18.10, 7.36, 13.62, 9.03};
		for (int i = 0; i < actual.length; i++) {
			Assert.assertEquals((Double) list.get(i).getField(1), actual[(int) list.get(i).getField(0)], 0.1);
		}
	}

}
