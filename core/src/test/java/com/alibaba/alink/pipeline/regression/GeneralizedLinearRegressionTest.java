package com.alibaba.alink.pipeline.regression;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp;
import com.alibaba.alink.operator.batch.regression.GlmPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.GlmTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.regression.glm.famliy.Tweedie;
import com.alibaba.alink.params.regression.GlmTrainParams;
import com.alibaba.alink.params.regression.GlmTrainParams.Family;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class GeneralizedLinearRegressionTest extends AlinkTestBase {

	@Test
	public void testAll() throws Exception {
		test(Family.Gamma, 8.717807596657824, false);
		test(Family.Gaussian, 2.0630965899629286, true);
		test(Family.Tweedie, 440.2158560037892, false);
		test(Family.Poisson, 0.7751000666424476, true);
		testBinomial();

		BatchOperator.execute();
	}

	public void test(Family family, double rmse, boolean fitIntercept) throws Exception {
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
			.setFamily(family)
			.setVariancePower(1.65)
			.setLinkPower(-0.5)
			.setRegParam(0.3)
			.setFitIntercept(false)
			.setMaxIter(10)
			.setOffsetCol("offset")
			.setWeightCol("weights")
			.setFitIntercept(false)
			.setFeatureCols(featureColNames)
			.setLabelCol(labelColName)
			.setPredictionCol(predictColName)
			.enableLazyPrintModelInfo();

		if (family.name().equals(new Tweedie(1).name())) {
			glm.setLink(GlmTrainParams.Link.Power);
		}

		GeneralizedLinearRegressionModel model = glm.fit(source);

		BatchOperator predict = model.transform(source);
		BatchOperator eval = model.evaluate(source);
		eval.lazyPrint(-1);

		EvalRegressionBatchOp evaluation = new EvalRegressionBatchOp()
			.setLabelCol(labelColName)
			.setPredictionCol(predictColName)
			.linkFrom(predict);

		evaluation.lazyCollectMetrics(
			d -> {
				Assert.assertEquals(rmse, d.getRmse(), 10e-3);
			}
		);
	}

	void testBinomial() {
		double[][] data = new double[][] {
			{1, 1, 1, 18},
			{2, 1, 2, 17},
			{3, 1, 3, 15},
			{4, 2, 1, 20},
			{5, 2, 2, 10},
			{6, 2, 3, 20},
			{7, 3, 1, 25},
			{8, 3, 2, 13},
			{9, 3, 3, 12}
		};

		List <Row> dataRow = new ArrayList <>();
		for (double[] aData : data) {
			dataRow.add(Row.of(aData[1], aData[2], aData[3] % 2, 1.0, 2.0));
		}

		String[] colNames = new String[] {"treatment", "outcome", "counts", "offset", "weights"};
		TypeInformation[] colTypes = new TypeInformation[] {Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
			Types.DOUBLE};

		BatchOperator source = new MemSourceBatchOp(dataRow,
			new TableSchema(colNames, colTypes));

		String[] featureColNames = new String[] {"treatment", "outcome"};
		String labelColName = "counts";

		GlmTrainBatchOp glmOp = new GlmTrainBatchOp()
			.setFamily("Binomial")
			.setLink("Logit")
			.setRegParam(0.3)
			.setFitIntercept(false)
			.setMaxIter(5)
			.setOffsetCol("offset")
			.setWeightCol("weights")
			.setFitIntercept(false)
			.setFeatureCols(featureColNames)
			.setLabelCol(labelColName);

		glmOp.linkFrom(source);

		GlmPredictBatchOp predict = new GlmPredictBatchOp()
			.setPredictionCol("pred");

		predict.linkFrom(glmOp, source);

		EvalRegressionBatchOp evaluation = new EvalRegressionBatchOp()
			.setLabelCol(labelColName)
			.setPredictionCol("pred")
			.linkFrom(predict);

		evaluation.lazyCollectMetrics(
			d -> {
				Assert.assertEquals(0.5042511014991389, d.getRmse(), 10e-3);
			}
		);
	}

}