package com.alibaba.alink.pipeline.regression;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.RegressionMetrics;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class GeneralizedLinearRegressionTest {

	@Test
	public void testGamma() throws Exception {

		double[][] data = new double[][] {{1, 5, 118, 69},
			{2, 10, 58, 35},
			{3, 15, 42, 26},
			{4, 20, 35, 21},
			{5, 30, 27, 18},
			{6, 40, 25, 16},
			{7, 60, 21, 13},
			{8, 80, 19, 12},
			{9, 100, 18, 12}
		};

		List <Row> dataRow = new ArrayList <>();
		for (double[] aData : data) {
			dataRow.add(Row.of(Math.log(aData[1]), aData[2], aData[3], 1.0, 2.0));
		}

		String[] colNames = new String[] {"u", "lot1", "lot2", "offset", "weights"};
		MemSourceBatchOp source = new MemSourceBatchOp(dataRow, colNames);

		String[] featureColNames = new String[] {"lot1", "lot2"};
		String labelColName = "u";
		String predictColName = "pred";

		GeneralizedLinearRegression glm = new GeneralizedLinearRegression()
			.setFamily("gamma")
			.setLink("Log")
			.setRegParam(0.3)
			.setFitIntercept(false)
			.setMaxIter(10)
			.setOffsetCol("offset")
			.setWeightCol("weights")
			.setFitIntercept(false)
			.setFeatureCols(featureColNames)
			.setLabelCol(labelColName)
			.setPredictionCol(predictColName);

		GeneralizedLinearRegressionModel model = glm.fit(source);
		BatchOperator predict = model.transform(source);

		EvalRegressionBatchOp evaluation = new EvalRegressionBatchOp()
			.setLabelCol(labelColName)
			.setPredictionCol(predictColName)
			.linkFrom(predict);

		RegressionMetrics metrics = evaluation.collectMetrics();

		Assert.assertEquals(0.7751000666424476, metrics.getRmse(), 10e-3);

		model.evaluate(source).print();
	}

}