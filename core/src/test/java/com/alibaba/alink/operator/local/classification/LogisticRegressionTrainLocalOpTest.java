package com.alibaba.alink.operator.local.classification;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.TableSourceLocalOp;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LogisticRegressionTrainLocalOpTest extends TestCase {
	@Test
	public void testLogisticRegressionTrainLocalOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of(2, 1, 1),
			Row.of(3, 2, 1),
			Row.of(4, 3, 2),
			Row.of(2, 4, 1),
			Row.of(2, 2, 1),
			Row.of(4, 3, 2),
			Row.of(1, 2, 1),
			Row.of(5, 3, 2)
		);
		LocalOperator <?> input = new TableSourceLocalOp(new MTable(df_data, "f0 int, f1 int, label int"));
		LocalOperator <?> lr = new LogisticRegressionTrainLocalOp()
			.setFeatureCols("f0", "f1")
			.setLabelCol("label")
			.lazyPrintModelInfo("model_info");
		LocalOperator <?> model = input.link(lr);
		LocalOperator <?> predictor = new LogisticRegressionPredictLocalOp()
			.setPredictionCol("pred");
		predictor.linkFrom(model, input).print();
	}
}