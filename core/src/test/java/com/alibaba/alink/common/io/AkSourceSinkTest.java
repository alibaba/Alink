package com.alibaba.alink.common.io;

import org.apache.flink.util.FileUtils;

import com.alibaba.alink.common.utils.testhttpsrc.Iris;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.AkSinkStreamOp;
import com.alibaba.alink.operator.stream.source.AkSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class AkSourceSinkTest extends AlinkTestBase {
	File path;

	@Before
	public void setup() throws Exception {
		String tmpDir = System.getProperty("java.io.tmpdir");
		path = new File(tmpDir, FileUtils.getRandomFilename("fs_test_stream"));
		if (!path.mkdirs()) {
			throw new RuntimeException("Fail to create dir: " + path);
		}
		testBatchSink();
		testStreamSink();
	}

	@After
	public void clear() throws Exception {
		FileUtils.deleteFileOrDirectory(path);
	}

	public void testBatchSink() throws Exception {
		BatchOperator data = Iris.getBatchData();

		data.link(new AkSinkBatchOp().setFilePath(new File(path, "af1").getAbsolutePath()).setOverwriteSink(true));
		data.link(new AkSinkBatchOp().setFilePath(new File(path, "ad2").getAbsolutePath()).setNumFiles(2)
			.setOverwriteSink(true));
		BatchOperator.execute();
	}

	public void testStreamSink() throws Exception {
		StreamOperator data = Iris.getStreamData();

		data.link(new AkSinkStreamOp().setFilePath(new File(path, "af1s").getAbsolutePath()).setOverwriteSink(true));
		data.link(new AkSinkStreamOp().setFilePath(new File(path, "ad2s").getAbsolutePath()).setNumFiles(2)
			.setOverwriteSink(true));
		StreamOperator.execute();
	}

	@Test
	public void testBatchSource() throws Exception {
		BatchOperator data1 = new AkSourceBatchOp().setFilePath(new File(path, "af1").getAbsolutePath());
		BatchOperator data5 = new AkSourceBatchOp().setFilePath(new File(path, "ad2").getAbsolutePath());
		data1.lazyPrint(4);
		data5.lazyPrint(4);
		BatchOperator.execute();
	}

	@Test
	public void testStreamSource() throws Exception {
		StreamOperator data1 = new AkSourceStreamOp().setFilePath(new File(path, "af1s").getAbsolutePath());
		StreamOperator data3 = new AkSourceStreamOp().setFilePath(new File(path, "ad2s").getAbsolutePath());
		data1.sample(0.1).print();
		data3.sample(0.1).print();
		StreamOperator.execute();
	}
}