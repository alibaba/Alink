package examples;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalMultiClassBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.MultiClassMetrics;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.classification.Softmax;

import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;

/**
 * Example for MultiClassEvaluation.
 */
public class MultiClassEvaluationExample {
	@Test
	public void main() throws Exception {
		Row[] data = new Row[] {
			Row.of(1.0, 7.0, 9.0, 2),
			Row.of(1.0, 3.0, 3.0, 3),
			Row.of(1.0, 2.0, 4.0, 1),
			Row.of(1.0, 2.0, 4.0, 1)
		};

		MemSourceBatchOp input = new MemSourceBatchOp(Arrays.asList(data), new String[] {"f0", "f1", "f2", "label"});

		Softmax lr = new Softmax()
			.setFeatureCols(new String[] {"f0", "f1", "f2"})
			.setStandardization(true)
			.setWithIntercept(true)
			.setEpsilon(1.0e-20)
			.setPredictionCol("pred")
			.setPredictionDetailCol("detail")
			.setLabelCol("label")
			.setMaxIter(10000);

		BatchOperator res = new Pipeline().add(lr).fit(input).transform(input);

		MultiClassMetrics evaluation = new EvalMultiClassBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detail")
			.linkFrom(res)
			.collectMetrics();

		System.out.println(evaluation.getAccuracy());
		System.out.println(evaluation.getMacroRecall());
		System.out.println(evaluation.getMacroPrecision());
		System.out.println(evaluation.getLogLoss());

	}
}
