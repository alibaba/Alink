package com.alibaba.alink.operator.stream.evaluation;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;

import java.util.Arrays;

public class EvalMultiClassStreamOpTest extends AlinkTestBase {
	@Test
	public void testDetailMulti() throws Exception {
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
			.setPredictionDetailCol("detailInput")
			.linkFrom(detailMultiTmp);
		CollectSinkStreamOp sink = new CollectSinkStreamOp()
			.linkFrom(op1);
		StreamOperator.execute();
		System.out.println(sink.getAndRemoveValues());
	}

	@Test
	public void testPredMulti() throws Exception {
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
			.setTimeInterval(0.01)
			.setLabelCol("label")
			.setPredictionCol("pred")
			.linkFrom(predMultiTmp);
		CollectSinkStreamOp sink = new CollectSinkStreamOp()
			.linkFrom(op1);
		StreamOperator.execute();
		System.out.println(sink.getAndRemoveValues());
	}

	/**
	 * In multi-classification evaluation, it is possible, in some time windows, only one label is encountered (both
	 * true value and prediction value).
	 * This case tests such situation.
	 * @throws Exception
	 */
	@Test
	public void testPredMultiOnlyOneLabel() throws Exception {
		Row[] predMultiArray =
			new Row[] {
				Row.of("prefix1", "prefix1"),
			};

		for (int i = 0; i < 10; i += 1) {    // 2 ^ 10 = 1024 times
			predMultiArray = ArrayUtils.addAll(predMultiArray, predMultiArray);
		}

		MemSourceStreamOp predMultiTmp = new MemSourceStreamOp(Arrays.asList(predMultiArray),
			new String[] {"label", "pred"});

		EvalMultiClassStreamOp op1 = new EvalMultiClassStreamOp()
			.setTimeInterval(0.01)
			.setLabelCol("label")
			.setPredictionCol("pred")
			.linkFrom(predMultiTmp);
		CollectSinkStreamOp sink = new CollectSinkStreamOp()
			.linkFrom(op1);
		StreamOperator.execute();
		System.out.println(sink.getAndRemoveValues());
	}
}
