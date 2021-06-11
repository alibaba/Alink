package com.alibaba.alink.operator.batch.evaluation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.BinaryClassMetrics;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;

/**
 * Unit test for BinaryClassEvaluation.
 */

public class EvalBinaryClassBatchOpTest extends AlinkTestBase {
	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void test() throws Exception {
		Row[] data =
			new Row[] {
				Row.of("prefix1", "{\"prefix1\": 0.9, \"prefix0\": 0.1}"),
				Row.of("prefix1", "{\"prefix1\": 0.8, \"prefix0\": 0.2}"),
				Row.of("prefix1", "{\"prefix1\": 0.7, \"prefix0\": 0.3}"),
				Row.of("prefix0", "{\"prefix1\": 0.75, \"prefix0\": 0.25}"),
				Row.of("prefix0", "{\"prefix1\": 0.6, \"prefix0\": 0.4}"),
				Row.of("prefix1", "{\"prefix1\": 0.65, \"prefix0\": 0.35}"),
				Row.of("prefix0", null),
				Row.of("prefix1", "{\"prefix1\": 0.55, \"prefix0\": 0.45}"),
				Row.of("prefix0", "{\"prefix1\": 0.4, \"prefix0\": 0.6}"),
				Row.of("prefix0", "{\"prefix1\": 0.3, \"prefix0\": 0.7}"),
				Row.of("prefix1", "{\"prefix1\": 0.35, \"prefix0\": 0.65}"),
				Row.of("prefix0", "{\"prefix1\": 0.2, \"prefix0\": 0.8}"),
				Row.of("prefix1", "{\"prefix1\": 0.1, \"prefix0\": 0.9}")
			};

		MemSourceBatchOp source = new MemSourceBatchOp(data, new String[] {"label", "detailInput"});

		BinaryClassMetrics metrics = new EvalBinaryClassBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detailInput")
			.linkFrom(source)
			.collectMetrics();

		Assert.assertEquals(0.769, metrics.getPrc(), 0.01);
		Assert.assertEquals(0.371, metrics.getKs(), 0.01);
		Assert.assertEquals(0.657, metrics.getAuc(), 0.01);
		Assert.assertEquals(0.666, metrics.getAccuracy(), 0.01);
		Assert.assertEquals(0.314,
			metrics.getMacroKappa(), 0.01);
		Assert.assertEquals(0.666, metrics.getMicroPrecision(), 0.01);
		Assert.assertEquals(0.666, metrics.getWeightedRecall(), 0.01);
		metrics.saveRocCurveAsImage(folder.getRoot().toPath() + "rocCurve.png", true);
		metrics.saveKSAsImage(folder.getRoot().toPath() + "ks.png", true);
		metrics.saveLiftChartAsImage(folder.getRoot().toPath() + "liftchart.png", true);
		metrics.saveRecallPrecisionCurveAsImage(folder.getRoot().toPath() + "recallPrecision.png", true);
		metrics.saveLorenzCurveAsImage(folder.getRoot().toPath() + "lorenzCurve.png", true);
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
			EvalBinaryClassBatchOp op = new EvalBinaryClassBatchOp()
				.setLabelCol("label")
				.setPredictionDetailCol("pred")
				.linkFrom(data);
			op.print();
			Assert.fail("Expected an IllegalStateException to be thrown");
		} catch (JobExecutionException | ProgramInvocationException e) {
			// pass
		}
	}
}