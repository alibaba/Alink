package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author weibo zhao
 * @date 17/06/2020 2:49 PM
 */
public class RegressionTest extends AlinkTestBase {

	@Test
	public void regression() throws Exception {
		TableSchema schema = new TableSchema(
			new String[] {"y", "x1", "x2"},
			new TypeInformation <?>[] {Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()}
		);
		double[][] data = new double[][] {
			{16.3, 1.1, 1.1},
			{16.8, 1.4, 1.5},
			{19.2, 1.7, 1.8},
			{18.0, 1.7, 1.7},
			{19.5, 1.8, 1.9},
			{20.9, 1.8, 1.8},
			{21.1, 1.9, 1.8},
			{20.9, 2.0, 2.1},
			{20.3, 2.3, 2.4},
			{22.0, 2.4, 2.5}
		};

		List <Row> rows = new ArrayList <>();

		for (double[] datarow : data) {
			rows.add(Row.of(new Object[] {datarow[0], datarow[1], datarow[2]}));
		}

		String[] xVars = new String[] {"x1", "x2"};
		String yVar = "y";

		MemSourceBatchOp mbts = new MemSourceBatchOp(rows, schema);

		Params bparams = new Params();
		bparams.set("featureColNames", xVars);
		bparams.set("labelColName", yVar);
		BatchOperator lintrain = new LinearRegTrainBatchOp(bparams).linkFrom(mbts);

		BatchOperator linsteptrain = new LinearRegStepwiseTrainBatchOp().setLabelCol(yVar).setFeatureCols(xVars)
			.setMethod("Forward").linkFrom(mbts);
		BatchOperator ridgetrain = new RidgeRegTrainBatchOp().setLabelCol(yVar).setFeatureCols(xVars).setLambda(
			0.5)
			.linkFrom(mbts);
		BatchOperator lassotrain = new LassoRegTrainBatchOp().setLabelCol(yVar).setFeatureCols(xVars)
			.setLambda(0.1).linkFrom(mbts);

		Params params = new Params();
		params.set("featureColNames", xVars);
		params.set("labelColName", yVar);
		params.set("C", 1.0);

		BatchOperator svr2train = new LinearSvrTrainBatchOp(params).setTau(0.01).linkFrom(mbts);

		BatchOperator pred1 = new LinearRegPredictBatchOp().setPredictionCol("linpred").linkFrom(lintrain, mbts);
		BatchOperator pred2 = new LinearRegStepwisePredictBatchOp().setPredictionCol("steppred").linkFrom(
			linsteptrain, pred1);
		BatchOperator pred3 = new RidgeRegPredictBatchOp().setPredictionCol("ridgepred").linkFrom(ridgetrain,
			pred2);
		BatchOperator pred4 = new LassoRegPredictBatchOp().setPredictionCol("lassopred").linkFrom(lassotrain,
			pred3);
		BatchOperator pred5 = new LinearSvrPredictBatchOp().setPredictionCol("svrpred").linkFrom(svr2train, pred4);

		pred5.print();
	}
}
