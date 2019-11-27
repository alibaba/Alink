package examples;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.BinaryClassMetrics;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.classification.NaiveBayes;

import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;

/**
 * Example for BinaryClassEvaluation.
 */
public class BinaryClassEvaluationExample {
	@Test
	public void main() {
		Row[] array = new Row[] {
			Row.of(new Object[] {"1.0 1.0 1.0 1.0", 1.0}),
			Row.of(new Object[] {"1.0 1.0 0.0 1.0", 1.0}),
			Row.of(new Object[] {"1.0 0.0 1.0 1.0", 1.0}),
			Row.of(new Object[] {"1.0 1.0 1.0 1.0", 1.0}),
			Row.of(new Object[] {"0.0 1.0 1.0 0.0", 0.0}),
			Row.of(new Object[] {"0.0 1.0 1.0 0.0", 0.0}),
			Row.of(new Object[] {"0.0 1.0 1.0 0.0", 0.0}),
			Row.of(new Object[] {"0.0 1.0 1.0 0.0", 0.0})
		};

		/* load training data */
		MemSourceBatchOp input = new MemSourceBatchOp(Arrays.asList(array), new String[] {"tensor", "labels"});

		/* train model */
		NaiveBayes nb = new NaiveBayes()
			.setModelType("Bernoulli")
			.setLabelCol("labels")
			.setPredictionCol("res")
			.setPredictionDetailCol("detail")
			.setVectorCol("tensor")
			.setSmoothing(0.5);

		BatchOperator prediction = new Pipeline().add(nb).fit(input).transform(input);

		EvalBinaryClassBatchOp evaluation = new EvalBinaryClassBatchOp()
			.setLabelCol("labels")
			.setPositiveLabelValueString("0.0")
			.setPredictionDetailCol("detail")
			.linkFrom(prediction);

		BinaryClassMetrics metrics = evaluation.collectMetrics();

		System.out.println(metrics.getAccuracy());

	}

}
