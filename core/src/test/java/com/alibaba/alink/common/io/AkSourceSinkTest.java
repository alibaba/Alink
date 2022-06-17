package com.alibaba.alink.common.io;

import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;

import com.alibaba.alink.common.utils.testhttpsrc.Iris;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.AkSinkStreamOp;
import com.alibaba.alink.operator.stream.source.AkSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

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

	@Test
	public void testBigRow() throws Exception {

		final int len = 0xFFFFFF;

		StringBuilder stringBuilder = new StringBuilder();

		for (int i = 0; i < (len - 1 - 5); ++i) {
			stringBuilder.append('a');
		}

		String big0 = stringBuilder.toString();

		stringBuilder.append('b');

		String big1 = stringBuilder.toString();

		MemSourceBatchOp memSourceBatchOp0 = new MemSourceBatchOp(new Row[]{
			Row.of(big0),
			Row.of(big1)
		}, new String[]{"big"});

		memSourceBatchOp0.link(new AkSinkBatchOp().setFilePath(new File(path, "big").getAbsolutePath()));

		BatchOperator.execute();

		new AkSourceBatchOp().setFilePath(new File(path, "big").getAbsolutePath())
			.lazyCollect(new Consumer <List <Row>>() {
				@Override
				public void accept(List <Row> rows) {
					rows.sort(new Comparator <Row>() {
						@Override
						public int compare(Row o1, Row o2) {
							return ((String) o1.getField(0)).compareTo((String) o2.getField(0));
						}
					});

					Assert.assertEquals((len - 1 - 5), ((String) rows.get(0).getField(0)).getBytes().length);
					Assert.assertEquals(len - 5, ((String) rows.get(1).getField(0)).getBytes().length);
				}
			});

		BatchOperator.execute();
	}
}