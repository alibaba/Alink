package com.alibaba.alink.operator.stream.evaluation;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.SpeedControlStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;

public class EvalRegressionStreamOpTest extends AlinkTestBase {
	Row[] array =
		new Row[] {
			Row.of(0.4, 0.5),
			Row.of(0.3, 0.5),
			Row.of(0.2, 0.6),
			Row.of(0.6, 0.7),
			Row.of(0.4, 0.5),
			Row.of(0.3, 0.5),
			Row.of(0.2, 0.6),
			Row.of(0.6, 0.7),
			Row.of(0.4, 0.5),
			Row.of(0.3, 0.5),
			Row.of(0.2, 0.6),
			Row.of(0.6, 0.7),
			Row.of(0.4, 0.5),
			Row.of(0.3, 0.5),
			Row.of(0.2, 0.6),
			Row.of(0.6, 0.7),
			Row.of(0.4, 0.5),
			Row.of(0.3, 0.5),
			Row.of(0.2, 0.6),
			Row.of(0.6, 0.7),
			Row.of(0.1, 0.5)
		};

	@Test
	public void testEvalRegression() throws Exception {
		MemSourceStreamOp dataStream = new MemSourceStreamOp(Arrays.asList(array), new String[] {"label", "pred"});
		EvalRegressionStreamOp op1 = new EvalRegressionStreamOp()
			.setLabelCol("label")
			.setTimeInterval(1)
			.setPredictionCol("pred");

		SpeedControlStreamOp controlStreamOp = new SpeedControlStreamOp()
			.setTimeInterval(0.02);

		dataStream.link(controlStreamOp).link(op1);

		StreamOperator.execute();
	}
}
