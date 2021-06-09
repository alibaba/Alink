package com.alibaba.alink.operator.stream.evaluation;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;

import java.util.Arrays;

public class EvalMultiClassStreamOpTest extends AlinkTestBase {
	@Test
	public void testDetailMulti() throws Exception {
		StreamOperator.setParallelism(2);
		Row[] detailMultiArray =
			new Row[] {
				Row.of("prefix0", "{\"prefix0\": 0.3, \"prefix1\": 0.2, \"prefix2\": 0.5}"),
				Row.of("prefix0", "{\"prefix0\": 0.4, \"prefix1\": 0.3, \"prefix2\": 0.3}"),
				Row.of("prefix1", "{\"prefix0\": 0.3, \"prefix1\": 0.4, \"prefix2\": 0.3}"),
				Row.of("prefix2", "{\"prefix0\": 0.5, \"prefix1\": 0.2, \"prefix2\": 0.3}"),
				Row.of("prefix2", "{\"prefix0\": 0.4, \"prefix1\": 0.3, \"prefix2\": 0.3}"),
				Row.of("prefix2", "{\"prefix0\": 0.4, \"prefix1\": 0.1, \"prefix2\": 0.5}"),
				Row.of("prefix2", "{\"prefix0\": 0.6, \"prefix1\": 0.1, \"prefix2\": 0.3}"),
				Row.of("prefix1", "{\"prefix0\": 0.4, \"prefix1\": 0.1, \"prefix2\": 0.5}"),
				Row.of("prefix1", "{\"prefix0\": 0.2, \"prefix1\": 0.5, \"prefix2\": 0.3}"),
				Row.of("prefix1", "{\"prefix0\": 0.2, \"prefix1\": 0.5, \"prefix2\": 0.3}"),
				Row.of("prefix1", "{\"prefix0\": 0.2, \"prefix1\": 0.5, \"prefix2\": 0.3}"),
				Row.of("prefix0", "{\"prefix0\": 0.4, \"prefix1\": 0.3, \"prefix2\": 0.3}"),
				Row.of("prefix0", "{\"prefix0\": 0.4, \"prefix1\": 0.1, \"prefix2\": 0.5}")
			};

		for (int i = 0; i < 10; i += 1) {    // 2 ^ 10 times
			detailMultiArray = ArrayUtils.addAll(detailMultiArray, detailMultiArray);
		}

		MemSourceStreamOp detailMultiTmp = new MemSourceStreamOp(Arrays.asList(detailMultiArray),
			new String[] {"label", "detailInput"});

		EvalMultiClassStreamOp op1 = new EvalMultiClassStreamOp()
			.setTimeInterval(0.01)
			.setLabelCol("label")
			.setPredictionDetailCol("detailInput");

		detailMultiTmp.link(op1).print();
		StreamOperator.execute();
	}

	@Test
	public void testPredMulti() throws Exception {
		StreamOperator.setParallelism(2);
		Row[] predMultiArray =
			new Row[] {
				Row.of("prefix1", "prefix1"),
				Row.of("prefix1", "prefix1"),
				Row.of("prefix0", "prefix0"),
				Row.of("prefix0", "prefix2"),
				Row.of("prefix2", "prefix0"),
				Row.of("prefix1", "prefix2"),
				Row.of("prefix1", "prefix1"),
				Row.of("prefix2", "prefix0"),
				Row.of("prefix2", "prefix0"),
				Row.of("prefix2", "prefix2"),
				Row.of("prefix0", "prefix2"),
				Row.of("prefix0", "prefix0"),
				Row.of("prefix1", "prefix1"),
				Row.of("prefix1", "prefix1"),
				Row.of("prefix1", "prefix1"),
				Row.of("prefix0", "prefix0"),
				Row.of("prefix0", "prefix2"),
				Row.of("prefix2", "prefix0"),
				Row.of("prefix1", "prefix2"),
				Row.of("prefix1", "prefix1"),
				Row.of("prefix2", "prefix0"),
				Row.of("prefix2", "prefix0"),
				Row.of("prefix2", "prefix2"),
				Row.of("prefix0", "prefix2"),
				Row.of("prefix0", "prefix0"),
				Row.of("prefix1", "prefix1"),
				Row.of("prefix1", "prefix1"),
				Row.of("prefix1", "prefix1"),
				Row.of("prefix0", "prefix0"),
				Row.of("prefix0", "prefix2"),
				Row.of("prefix2", "prefix0"),
				Row.of("prefix1", "prefix2"),
				Row.of("prefix1", "prefix1"),
				Row.of("prefix2", "prefix0"),
				Row.of("prefix2", "prefix0"),
				Row.of("prefix2", "prefix2"),
				Row.of("prefix0", "prefix2"),
				Row.of("prefix0", "prefix0"),
				Row.of("prefix1", "prefix1"),
				Row.of("prefix1", "prefix1"),
				Row.of("prefix1", "prefix1"),
				Row.of("prefix0", "prefix0"),
				Row.of("prefix0", "prefix2"),
				Row.of("prefix2", "prefix0"),
				Row.of("prefix1", "prefix2"),
				Row.of("prefix1", "prefix1"),
				Row.of("prefix2", "prefix0"),
				Row.of("prefix2", "prefix0"),
				Row.of("prefix2", "prefix2"),
				Row.of("prefix0", "prefix2"),
				Row.of("prefix0", "prefix0"),
				Row.of("prefix1", "prefix1")
			};

		for (int i = 0; i < 10; i += 1) {    // 2 ^ 10 = 1024 times
			predMultiArray = ArrayUtils.addAll(predMultiArray, predMultiArray);
		}

		MemSourceStreamOp predMultiTmp = new MemSourceStreamOp(Arrays.asList(predMultiArray),
			new String[] {"label", "pred"});

		EvalMultiClassStreamOp op1 = new EvalMultiClassStreamOp()
			.setTimeInterval(0.5)
			.setLabelCol("label")
			.setPredictionCol("pred");

		predMultiTmp.link(op1).print();

		StreamOperator.execute();
	}
}
