package com.alibaba.alink;

import com.alibaba.alink.common.io.filesystem.BaseFileSystem;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.filesystem.HadoopFileSystem;
import com.alibaba.alink.common.io.filesystem.OssFileSystem;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CsvSinkStreamOp;
import com.alibaba.alink.operator.stream.source.AkSourceStreamOp;

/**
 * Example for Ak source/sink and file system.
 */
public class AkExample {
	public static void main(String[] args) throws Exception {
		String URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv";
		String SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";

		// Note: Complete the parameter below with the right oss configure.
		BaseFileSystem<?> ossFileSystem = new OssFileSystem(
			"OssEndPoint", "OssBucket", "OssId", "OssKey"
		);

		// Note: Complete the parameter below with the right hdfs configure.
		BaseFileSystem hadoopFileSystem = new HadoopFileSystem("HdfsFileSystemUri");

		// csv to oss
		CsvSourceBatchOp csvSourceBatchOp = new CsvSourceBatchOp()
			.setFilePath(URL)
			.setSchemaStr(SCHEMA_STR);

		AkSinkBatchOp akSinkToOss = new AkSinkBatchOp()
			.setFilePath(new FilePath("iris", ossFileSystem))
			.setOverwriteSink(true);

		csvSourceBatchOp.link(akSinkToOss);

		BatchOperator.execute();

		// oss to hdfs
		AkSourceBatchOp akSourceFromOss = new AkSourceBatchOp()
			.setFilePath(new FilePath("iris", ossFileSystem));

		AkSinkBatchOp akSinkToHdfs = new AkSinkBatchOp()
			.setFilePath(new FilePath("iris", hadoopFileSystem))
			.setOverwriteSink(true);

		akSourceFromOss.link(akSinkToHdfs);

		BatchOperator.execute();

		// hdfs to stdout
		AkSourceBatchOp akSourceFromHdfs = new AkSourceBatchOp()
			.setFilePath(new FilePath("iris", hadoopFileSystem));

		akSourceFromHdfs.firstN(10).print();
	}
}
