package examples;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.RegressionMetrics;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.feature.Binarizer;
import com.alibaba.alink.pipeline.regression.AftSurvivalRegression;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;

/**
 * Example for RegressionEvaluation.
 */
public class RegressionEvaluationExample {
	@Test
	public void main() throws Exception {
		Row[] rows = new Row[] {
			Row.of(new Object[] {1.218, 16.0, "1.560 -0.605"}),
			Row.of(new Object[] {2.949, 4.0, "0.346 2.158"}),
			Row.of(new Object[] {3.627, 2.0, "1.380 0.231"}),
			Row.of(new Object[] {0.273, 15.0, "0.520 1.151"}),
			Row.of(new Object[] {4.199, 7.0, "0.795 -0.226"})
		};

		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(rows), new String[] {"label", "censor", "features"});

		Binarizer binarizer = new Binarizer()
			.setSelectedCol("censor")
			.setThreshold(8.0);

		AftSurvivalRegression reg = new AftSurvivalRegression()
			.setVectorCol("features")
			.setLabelCol("label")
			.setCensorCol("censor")
			.setPredictionCol("result");

		Pipeline pipeline = new Pipeline().add(binarizer).add(reg);

		BatchOperator res = pipeline.fit(data).transform(data);

		EvalRegressionBatchOp evaluation = new EvalRegressionBatchOp()
			.setLabelCol("label")
			.setPredictionCol("result")
			.linkFrom(res);

		RegressionMetrics metrics = evaluation.collectMetrics();

		System.out.println(metrics.getRmse());
		System.out.println(metrics.getMape());
		System.out.println(metrics.getMae());
	}
}
