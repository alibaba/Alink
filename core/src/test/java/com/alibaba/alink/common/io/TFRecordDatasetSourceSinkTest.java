package com.alibaba.alink.common.io;

import org.apache.flink.util.FileUtils;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.utils.testhttpsrc.Iris;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.TFRecordDatasetSinkBatchOp;
import com.alibaba.alink.operator.batch.source.TFRecordDatasetSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.TFRecordDatasetSinkStreamOp;
import com.alibaba.alink.operator.stream.source.TFRecordDatasetSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class TFRecordDatasetSourceSinkTest extends AlinkTestBase {
	Path directory;
	Path singleFileSourcePath;
	Path multiFilesSourcePath;

	@Before
	public void setup() throws Exception {

		directory = Files.createTempDirectory("test-tfrecord-dataset");

		singleFileSourcePath = directory.resolve("file");
		Files.copy(Objects.requireNonNull(getClass().getResourceAsStream("/tfrecord_dataset/file")),
			singleFileSourcePath);

		multiFilesSourcePath = directory.resolve("folder");
		Files.createDirectory(multiFilesSourcePath);
		Files.copy(Objects.requireNonNull(getClass().getResourceAsStream("/tfrecord_dataset/folder/1")),
			multiFilesSourcePath.resolve("1"));
		Files.copy(Objects.requireNonNull(getClass().getResourceAsStream("/tfrecord_dataset/folder/2")),
			multiFilesSourcePath.resolve("2"));
	}

	@After
	public void clear() throws Exception {
		FileUtils.deleteDirectoryQuietly(directory.toFile());
	}

	@Test
	public void testBatchSink() throws Exception {
		BatchOperator <?> source = Iris.getBatchData();

		source.link(new TFRecordDatasetSinkBatchOp()
			.setFilePath(directory.resolve("single_file_batch").toFile().getAbsolutePath())
			.setOverwriteSink(true));
		source.link(new TFRecordDatasetSinkBatchOp()
			.setFilePath(directory.resolve("multi_files_batch").toFile().getAbsolutePath())
			.setNumFiles(2)
			.setOverwriteSink(true));
		BatchOperator.execute();
	}

	@Test
	public void testStreamSink() throws Exception {
		StreamOperator <?> data = Iris.getStreamData();

		data.link(new TFRecordDatasetSinkStreamOp()
			.setFilePath(directory.resolve("single_file_stream").toFile().getAbsolutePath())
			.setOverwriteSink(true));
		data.link(new TFRecordDatasetSinkStreamOp()
			.setFilePath(directory.resolve("multi_files_stream").toFile().getAbsolutePath())
			.setNumFiles(2)
			.setOverwriteSink(true));
		StreamOperator.execute();
	}

	@Test
	public void testBatchSource() throws Exception {
		BatchOperator <?> singleFileSource = new TFRecordDatasetSourceBatchOp()
			.setFilePath(singleFileSourcePath.toFile().getAbsolutePath())
			.setSchemaStr(TableUtil.schema2SchemaStr(Iris.getBatchData().getSchema()));
		BatchOperator <?> multiFilesSource = new TFRecordDatasetSourceBatchOp()
			.setFilePath(multiFilesSourcePath.toFile().getAbsolutePath())
			.setSchemaStr(TableUtil.schema2SchemaStr(Iris.getBatchData().getSchema()));
		singleFileSource.lazyPrint(4);
		multiFilesSource.lazyPrint(4);
		BatchOperator.execute();
	}

	@Test
	public void testStreamSource() throws Exception {
		StreamOperator <?> singleFileSource = new TFRecordDatasetSourceStreamOp()
			.setFilePath(singleFileSourcePath.toFile().getAbsolutePath())
			.setSchemaStr(TableUtil.schema2SchemaStr(Iris.getBatchData().getSchema()));
		StreamOperator <?> multiFilesSource = new TFRecordDatasetSourceStreamOp()
			.setFilePath(multiFilesSourcePath.toFile().getAbsolutePath())
			.setSchemaStr(TableUtil.schema2SchemaStr(Iris.getBatchData().getSchema()));
		singleFileSource.print();
		multiFilesSource.print();
		StreamOperator.execute();
	}
}
