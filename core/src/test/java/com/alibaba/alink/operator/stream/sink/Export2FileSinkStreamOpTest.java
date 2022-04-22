package com.alibaba.alink.operator.stream.sink;

import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.RandomTableSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.*;

public class Export2FileSinkStreamOpTest extends AlinkTestBase {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void linkFrom() throws Exception {
		String filePath = temporaryFolder.getRoot().getPath();

		System.out.println(filePath);

		//RandomTableSourceStreamOp randomTableSourceStreamOp = new RandomTableSourceStreamOp()
		//	.setIdCol("id")
		//	.setNumCols(3)
		//	.setTimePerSample(1.0)
		//	.setMaxRows(10000L);

		RandomTableSourceStreamOp randomTableSourceStreamOp = new RandomTableSourceStreamOp()
			.setIdCol("id")
			.setNumCols(3)
			.setMaxRows(1000L);

		Export2FileSinkStreamOp export2FileSinkStreamOp = new Export2FileSinkStreamOp()
			.setFilePath(filePath)
			.setWindowTime(1)
			.setOverwriteSink(true);

		export2FileSinkStreamOp.linkFrom(randomTableSourceStreamOp);

		StreamOperator.execute();
	}

	@Ignore
	@Test
	public void testBatchRead() throws Exception {
		String filePath = "/var/folders/_r/nyqgr4sx18z38mfthhz3wnzc0000gp/T/junit1027364942484077517";

		new AkSourceBatchOp()
			.setFilePath(filePath)
			.print();
	}
}