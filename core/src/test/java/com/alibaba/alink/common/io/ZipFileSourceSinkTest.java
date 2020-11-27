package com.alibaba.alink.common.io;

import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import com.alibaba.alink.testutil.categories.DbTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.io.File;

public class ZipFileSourceSinkTest extends AlinkTestBase {
	private String path;
	private BatchOperator data;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Before
	public void setup() {
		path = FileUtils.getRandomFilename("__temp");
		if (!new File(path).mkdirs()) {
			throw new RuntimeException("Fail to create dir: " + path);
		}

		Row[] rows = new Row[] {
			Row.of("0L", "1L", 0.6),
			Row.of("2L", "2L", 0.8),
			Row.of("2L", "4L", 0.6),
			Row.of("3L", "1L", 0.6),
			Row.of("3L", "2L", 0.3),
			Row.of("3L", "4L", 0.4),
		};

		data = new MemSourceBatchOp(rows, new String[] {"uid", "iid", "label"});
	}

	@After
	public void clear() throws Exception {
		FileUtils.deleteFileOrDirectory(new File(path));
	}

	@Category(DbTest.class)
	@Test
	public void testBatchSourceSinkSingleFile() throws Exception {
		String filePath = path + "/file1.zip";
		data.link(new AkSinkBatchOp().setFilePath(filePath).setOverwriteSink(true));
		BatchOperator.execute();

		BatchOperator source = new AkSourceBatchOp().setFilePath(filePath);
		Assert.assertEquals(source.count(), 6);
	}
}