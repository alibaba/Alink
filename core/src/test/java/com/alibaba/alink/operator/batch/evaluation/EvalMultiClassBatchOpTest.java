package com.alibaba.alink.operator.batch.evaluation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkFlinkExecutionErrorException;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.MultiClassMetrics;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Unit test for MultiClassEvaluation.
 */

public class EvalMultiClassBatchOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {

		Row[] data =
			new Row[] {
				Row.of("prefix0", "{\"prefix0\": 0.3, \"prefix1\": 0.2, \"prefix2\": 0.5}"),
				Row.of("prefix0", "{\"prefix0\": 0.4, \"prefix1\": 0.3, \"prefix2\": 0.3}"),
				Row.of("prefix1", "{\"prefix0\": 0.3, \"prefix1\": 0.4, \"prefix2\": 0.3}"),
				Row.of("prefix2", "{\"prefix0\": 0.5, \"prefix1\": 0.2, \"prefix2\": 0.3}"),
				Row.of("prefix2", "{\"prefix0\": 0.4, \"prefix1\": 0.3, \"prefix2\": 0.3}"),
				Row.of("prefix2", "{\"prefix0\": 0.4, \"prefix1\": 0.1, \"prefix2\": 0.5}"),
				Row.of("prefix2", "{\"prefix0\": 0.6, \"prefix1\": 0.1, \"prefix2\": 0.3}"),
				Row.of("prefix1", null),
				Row.of("prefix1", "{\"prefix0\": 0.4, \"prefix1\": 0.1, \"prefix2\": 0.5}"),
				Row.of("prefix1", "{\"prefix0\": 0.2, \"prefix1\": 0.5, \"prefix2\": 0.3}"),
				Row.of("prefix1", "{\"prefix0\": 0.2, \"prefix1\": 0.5, \"prefix2\": 0.3}"),
				Row.of("prefix1", "{\"prefix0\": 0.2, \"prefix1\": 0.5, \"prefix2\": 0.3}"),
				Row.of("prefix0", "{\"prefix0\": 0.4, \"prefix1\": 0.3, \"prefix2\": 0.3}"),
				Row.of("prefix0", "{\"prefix0\": 0.4, \"prefix1\": 0.1, \"prefix2\": 0.5}")
			};

		MemSourceBatchOp source = new MemSourceBatchOp(data, new String[] {"label", "detailInput"});

		MultiClassMetrics metrics = new EvalMultiClassBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detailInput")
			.linkFrom(source)
			.collectMetrics();

		Assert.assertEquals(0.538, metrics.getAccuracy(), 0.01);
		Assert.assertEquals(0.301, metrics.getMacroKappa(), 0.01);
		Assert.assertEquals(0.538, metrics.getMicroPrecision(), 0.01);
		Assert.assertEquals(0.538, metrics.getWeightedRecall(), 0.01);
		Assert.assertEquals(0.444, metrics.getF1("prefix0"), 0.01);
		Assert.assertEquals(1.042, metrics.getLogLoss(), 0.01);
		Assert.assertEquals(0.538, metrics.getMicroSensitivity(), 0.01);
		Assert.assertEquals(0.777, metrics.getMacroSpecificity(), 0.01);

		System.out.println(metrics.toString());
	}

	@Test
	public void testEmptyInput() throws Exception {
		Row[] rows = new Row[] {
			Row.of(null, null),
			Row.of("1", null),
			Row.of(null, "1")
		};

		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(rows),
			new TableSchema(new String[] {"pred", "label"},
				new TypeInformation[] {Types.STRING, Types.STRING}));
		try {
			EvalMultiClassBatchOp op = new EvalMultiClassBatchOp()
				.setLabelCol("label")
				.setPredictionCol("pred")
				.linkFrom(data);
			op.print();
			Assert.fail("Expected an IllegalStateException to be thrown");
		} catch (JobExecutionException | ProgramInvocationException | AkFlinkExecutionErrorException e) {
			// pass
		}

	}
}