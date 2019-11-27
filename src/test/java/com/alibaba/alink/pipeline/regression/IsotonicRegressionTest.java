package com.alibaba.alink.pipeline.regression;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Test for IsotonicRegression.
 */
public class IsotonicRegressionTest {
	private Row[] rows = new Row[] {
			Row.of(0.35, 1),
			Row.of(0.6, 1),
			Row.of(0.55, 1),
			Row.of(0.5, 1),
			Row.of(0.18, 0),
			Row.of(0.1, 1),
			Row.of(0.8, 1),
			Row.of(0.45, 0),
			Row.of(0.4, 1),
			Row.of(0.7, 0),
			Row.of(0.02, 1),
			Row.of(0.3, 0),
			Row.of(0.27, 1),
			Row.of(0.2, 0),
			Row.of(0.9, 1)
	};
	private Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"feature", "label"});
	private Table dataStream = MLEnvironmentFactory.getDefault().createStreamTable(rows, new String[] {"feature", "label"});

	@Test
	public void testIsotonicReg() throws Exception {
		IsotonicRegression op = new IsotonicRegression()
			.setFeatureCol("feature")
			.setLabelCol("label")
			.setPredictionCol("result");

		PipelineModel model = new Pipeline().add(op).fit(data);

		Table res = model.transform(data);

		List <Double> list = MLEnvironmentFactory.getDefault().getBatchTableEnvironment().toDataSet(res.select("result"), Double.class)
			.collect();
		double[] actual = new double[] {0.66, 0.75, 0.75, 0.75, 0.5, 0.5, 0.75, 0.66, 0.66, 0.75, 0.5, 0.5, 0.5, 0.5,
			0.75};
		for (int i = 0; i < actual.length; i++) {
			Assert.assertEquals(list.get(i), actual[i], 0.01);
		}

		res = model.transform(dataStream);

		DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,res).print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}




}
