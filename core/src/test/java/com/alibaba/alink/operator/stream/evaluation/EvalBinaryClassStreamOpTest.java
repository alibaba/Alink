package com.alibaba.alink.operator.stream.evaluation;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;

public class EvalBinaryClassStreamOpTest extends AlinkTestBase {
	@Test
	public void testDetailBinary() throws Exception {
		Row[] detailBinaryArray =
			new Row[] {
				Row.of("prefix1", "{\"prefix1\": 0.1, \"prefix0\": 0.9}"),
				Row.of("prefix1", "{\"prefix1\": 0.8, \"prefix0\": 0.2}"),
				Row.of("prefix1", "{\"prefix1\": 0.4, \"prefix0\": 0.6}"),
				Row.of("prefix0", "{\"prefix1\": 0.75, \"prefix0\": 0.25}"),
				Row.of("prefix0", "{\"prefix1\": 0.6, \"prefix0\": 0.4}"),
				Row.of("prefix1", "{\"prefix1\": 0.65, \"prefix0\": 0.35}"),
				Row.of("prefix1", "{\"prefix1\": 0.55, \"prefix0\": 0.45}"),
				Row.of("prefix0", "{\"prefix1\": 0.1, \"prefix0\": 0.9}"),
				Row.of("prefix0", "{\"prefix1\": 0.3, \"prefix0\": 0.7}"),
				Row.of("prefix1", "{\"prefix1\": 0.25, \"prefix0\": 0.75}"),
				Row.of("prefix0", "{\"prefix1\": 0.2, \"prefix0\": 0.8}"),
				Row.of("prefix1", "{\"prefix1\": 0.1, \"prefix0\": 0.9}")
			};

		MemSourceStreamOp detailBinaryTmp = new MemSourceStreamOp(Arrays.asList(detailBinaryArray),
			new String[] {"label", "detailInput"});

		EvalBinaryClassStreamOp op1 = new EvalBinaryClassStreamOp()
			.setLabelCol("label")
			.setPositiveLabelValueString("prefix0")
			.setTimeInterval(0.001)
			.setPredictionDetailCol("detailInput");

		detailBinaryTmp.link(op1).print();

		StreamOperator.execute();
	}
}
