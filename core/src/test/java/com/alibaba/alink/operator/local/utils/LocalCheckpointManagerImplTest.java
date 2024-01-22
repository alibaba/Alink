package com.alibaba.alink.operator.local.utils;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.operator.common.evaluation.BinaryClassMetrics;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.dataproc.SampleLocalOp;
import com.alibaba.alink.operator.local.dataproc.SplitLocalOp;
import com.alibaba.alink.operator.local.evaluation.EvalBinaryClassLocalOp;
import com.alibaba.alink.operator.local.feature.BinarizerLocalOp;
import com.alibaba.alink.operator.local.sink.AkSinkLocalOp;
import com.alibaba.alink.operator.local.source.CsvSourceLocalOp;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

public class LocalCheckpointManagerImplTest extends TestCase {

	static String CHECKPOINT_DIR_PATH;

	@Ignore
	public void test_1() throws IOException {
		CHECKPOINT_DIR_PATH = Files.createTempDirectory("ckpt_").toAbsolutePath().toString();
		System.out.println(CHECKPOINT_DIR_PATH);

		AlinkGlobalConfiguration.setPrintProcessInfo(true);

		execWithCheckpoint(CHECKPOINT_DIR_PATH);

		new LocalCheckpointManagerImpl(CHECKPOINT_DIR_PATH).clearNodesAfter("sample");

		execWithCheckpoint(CHECKPOINT_DIR_PATH);
	}

	@Test
	public void testEval() throws Exception {

		CHECKPOINT_DIR_PATH = Files.createTempDirectory("ckpt_").toAbsolutePath().toString();
		System.out.println(CHECKPOINT_DIR_PATH);
		AlinkGlobalConfiguration.setPrintProcessInfo(true);

		execEval(CHECKPOINT_DIR_PATH);


		execEval(CHECKPOINT_DIR_PATH);
	}

	private static void execEval(String checkpointDirPath) {
		LocalCheckpointManagerImpl lcm = new LocalCheckpointManagerImpl(checkpointDirPath);


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

		BinaryClassMetrics metrics = new MemSourceLocalOp(data, new String[] {"label", "detailInput"})
			.link(
				new EvalBinaryClassLocalOp()
					.setLabelCol("label")
					.setPredictionDetailCol("detailInput")
					.enableCheckpoint(lcm, "eval")
			)
			.collectMetrics();

		Assert.assertEquals(0.769, metrics.getPrc(), 0.01);
		Assert.assertEquals(0.371, metrics.getKs(), 0.01);
		Assert.assertEquals(0.657, metrics.getAuc(), 0.01);
		Assert.assertEquals(0.666, metrics.getAccuracy(), 0.01);
		Assert.assertEquals(0.314, metrics.getMacroKappa(), 0.01);
		Assert.assertEquals(0.666, metrics.getMicroPrecision(), 0.01);
		Assert.assertEquals(0.666, metrics.getWeightedRecall(), 0.01);
	}


	private static void execWithCheckpoint(String checkpointDirPath) {
		LocalCheckpointManagerImpl lcm = new LocalCheckpointManagerImpl(checkpointDirPath);

		CsvSourceLocalOp source = new CsvSourceLocalOp()
			.setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
			.setSchemaStr(
				"sepal_length double, sepal_width double, petal_length double, petal_width double, category string")
			.enableCheckpoint(lcm, "source");

		SplitLocalOp spliter = new SplitLocalOp()
			.setFraction(0.8)
			.enableCheckpoint(lcm, "split");

		source.link(spliter);

		spliter.printStatistics();

		spliter.getSideOutput(0)
			.printStatistics()
			.link(
				new SampleLocalOp()
					.setRatio(0.5)
					.enableCheckpoint(lcm, "sample")
			)
			.lazyPrint()
			.printStatistics()
			.link(
				new BinarizerLocalOp()
					.setSelectedCol("sepal_length")
					.setThreshold(6.0)
					.enableCheckpoint(lcm, "binary")
			)
			.link(
				new AkSinkLocalOp()
					.setFilePath(checkpointDirPath + File.separator + "sink.ak")
			);

		LocalOperator.execute();

		lcm.printStatus();
		System.out.println(Arrays.toString(new File(checkpointDirPath).list()));
		System.out.println("\n######################  End of execWithCheckpoint  ########################\n\n");
	}
}