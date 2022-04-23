package com.alibaba.alink;

import com.alibaba.alink.operator.batch.source.*;
import com.alibaba.alink.operator.stream.sink.Export2FileSinkStreamOp;
import com.alibaba.alink.operator.stream.source.ParquetSourceStreamOp;
import com.alibaba.alink.operator.stream.source.TsvSourceStreamOp;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.filesystem.HadoopFileSystem;
import com.alibaba.alink.common.io.filesystem.LocalFileSystem;
import com.alibaba.alink.common.io.filesystem.OssFileSystem;
import com.alibaba.alink.common.io.plugin.PluginDownloader;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorNormalizeBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.AkSinkStreamOp;
import com.alibaba.alink.operator.stream.sink.CsvSinkStreamOp;
import com.alibaba.alink.operator.stream.source.AkSourceStreamOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.table.functions.ScalarFunction;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.List;

/**
 */
public class Chap03 {

	static final String LOCAL_DIR = Utils.ROOT_DIR + "filesys" + File.separator;

	static final String HADOOP_VERSION = "2.8.3";
	static final String OSS_VERSION = "3.4.1";

	static final String OSS_END_POINT = "*";
	static final String OSS_BUCKET_NAME = "*";
	static final String OSS_ACCESS_ID = "*";
	static final String OSS_ACCESS_KEY = "*";

	static final String OSS_PREFIX_URI = "oss://" + OSS_BUCKET_NAME + "/";

	static final String HDFS_URI = "hdfs://10.*.*.*:9000/";

	static final String IRIS_HTTP_URL =
		"http://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data";

	static final String IRIS_SCHEMA_STR =
		"sepal_length double, sepal_width double, petal_length double, petal_width double, category string";

	static final String ALINK_PLUGIN_DIR = "/Users/yangxu/Downloads/alink_plugin";

	public static void main(String[] args) throws Exception {

		System.out.print("Flink version: ");
		System.out.println(AlinkGlobalConfiguration.getFlinkVersion());

		System.out.print("Default Plugin Dir: ");
		System.out.println(new File(AlinkGlobalConfiguration.getPluginDir()).getCanonicalPath());

		AlinkGlobalConfiguration.setPluginDir(ALINK_PLUGIN_DIR);

		System.out.print("Current Plugin Dir: ");
		System.out.println(new File(AlinkGlobalConfiguration.getPluginDir()).getCanonicalPath());

		PluginDownloader downloader = AlinkGlobalConfiguration.getPluginDownloader();

		List <String> pluginNames = downloader.listAvailablePlugins();
		for (String pluginName : pluginNames) {
			List<String> versions = downloader.listAvailablePluginVersions(pluginName);
			System.out.println(pluginName + " => " + ArrayUtils.toString(versions));
		}

		downloader.downloadPlugin("mysql", "5.1.27");

		downloader.downloadAll();

		BatchOperator.setParallelism(1);

		c_1_1();

		c_1_2_1();

		c_1_2_2();

		c_1_3_1();

		c_1_3_2();

		c_2_1_1();

		c_2_1_2();

		c_2_2();

		c_2_3_1();

		c_2_4();

		c_2_5_1();

		c_2_5_2();
	}

	static void c_1_1() throws Exception {

		LocalFileSystem local = new LocalFileSystem();
		System.out.println(local.getHomeDirectory());
		System.out.println(local.getKind());

		if (!local.exists(LOCAL_DIR)) {
			local.mkdirs(LOCAL_DIR);
		}

		for (FileStatus status : local.listStatus(LOCAL_DIR)) {
			System.out.println(status.getPath().toUri()
				+ " \t" + status.getLen()
				+ " \t" + new Date(status.getModificationTime())
			);
		}

		String path = LOCAL_DIR + "hello.txt";

		OutputStream outputStream = local.create(path, WriteMode.OVERWRITE);
		outputStream.write("Hello Alink!".getBytes());
		outputStream.close();

		FileStatus status = local.getFileStatus(path);
		System.out.println(status);
		System.out.println(status.getLen());
		System.out.println(new Date(status.getModificationTime()));

		InputStream inputStream = local.open(path);
		String readString = IOUtils.toString(inputStream);
		System.out.println(readString);
	}

	static void c_1_2_1() throws Exception {
		HadoopFileSystem hdfs = new HadoopFileSystem(HADOOP_VERSION, HDFS_URI);

		final String hdfsDir = HDFS_URI + "user/yangxu/alink/data/temp/";

		System.out.println(hdfs.getKind());

		if (!hdfs.exists(hdfsDir)) {
			hdfs.mkdirs(hdfsDir);
		}

		String path = hdfsDir + "hello.txt";

		if (hdfs.exists(path)) {
			hdfs.delete(path, true);
		}

		OutputStream outputStream = hdfs.create(path, WriteMode.NO_OVERWRITE);
		outputStream.write("Hello Alink!".getBytes());
		outputStream.close();

		InputStream inputStream = hdfs.open(path);
		String readString = IOUtils.toString(inputStream);
		System.out.println(readString);
	}

	static void c_1_2_2() throws Exception {
		LocalFileSystem local = new LocalFileSystem();

		HadoopFileSystem hdfs = new HadoopFileSystem(HADOOP_VERSION, HDFS_URI);

		copy(
			hdfs.open(HDFS_URI + "user/yangxu/alink/data/temp/hello.txt"),
			local.create(LOCAL_DIR + "hello_1.txt", WriteMode.OVERWRITE)
		);

		copy(
			local.open(LOCAL_DIR + "hello_1.txt"),
			hdfs.create(HDFS_URI + "user/yangxu/alink/data/temp/hello_2.txt", WriteMode.OVERWRITE)
		);

		for (FileStatus status : hdfs.listStatus(HDFS_URI + "user/yangxu/alink/data/temp/")) {
			System.out.println(status.getPath().toUri()
				+ " \t" + status.getLen()
				+ " \t" + new Date(status.getModificationTime())
			);
		}

	}

	static void c_1_3_1() throws Exception {
		OssFileSystem oss =
			new OssFileSystem(
				OSS_VERSION,
				OSS_END_POINT,
				OSS_BUCKET_NAME,
				OSS_ACCESS_ID,
				OSS_ACCESS_KEY
			);

		System.out.println(oss.getKind());

		final String ossDir = OSS_PREFIX_URI + "alink/data/temp/";

		if (!oss.exists(new Path(ossDir))) {
			oss.mkdirs(new Path(ossDir));
		}

		String path = ossDir + "hello.txt";

		OutputStream outputStream = oss.create(path, WriteMode.OVERWRITE);
		outputStream.write("Hello Alink!".getBytes());
		outputStream.close();

		InputStream inputStream = oss.open(path);
		String readString = IOUtils.toString(inputStream);
		System.out.println(readString);
	}

	static void c_1_3_2() throws Exception {
		LocalFileSystem local = new LocalFileSystem();

		OssFileSystem oss =
			new OssFileSystem(
				OSS_VERSION,
				OSS_END_POINT,
				OSS_BUCKET_NAME,
				OSS_ACCESS_ID,
				OSS_ACCESS_KEY
			);

		copy(
			oss.open(OSS_PREFIX_URI + "alink/data/temp/hello.txt"),
			local.create(LOCAL_DIR + "hello_1.txt", WriteMode.OVERWRITE)
		);

		copy(
			local.open(LOCAL_DIR + "hello_1.txt"),
			oss.create(OSS_PREFIX_URI + "alink/data/temp/hello_2.txt", WriteMode.OVERWRITE)
		);

		for (FileStatus status : oss.listStatus(new Path(OSS_PREFIX_URI + "alink/data/temp/"))) {
			System.out.println(status.getPath().toUri()
				+ " \t" + status.getLen()
				+ " \t" + new Date(status.getModificationTime())
			);
		}

	}

	static void copy(InputStream in, OutputStream out) throws IOException {
		byte[] buffer = new byte[1024 * 1024];
		int len = in.read(buffer);
		while (len != -1) {
			out.write(buffer, 0, len);
			len = in.read(buffer);
		}
		in.close();
		out.close();
	}

	static void c_2_1_1() throws Exception {
		CsvSourceBatchOp source_local = new CsvSourceBatchOp()
			.setFilePath(LOCAL_DIR + "iris.data")
			.setSchemaStr("sepal_length double, sepal_width double, "
				+ "petal_length double, petal_width double, category string");

		source_local.firstN(5).print();

		CsvSourceBatchOp source_url = new CsvSourceBatchOp()
			.setFilePath("http://archive.ics.uci.edu/ml/machine-learning-databases"
				+ "/iris/iris.data")
			.setSchemaStr("sepal_length double, sepal_width double, "
				+ "petal_length double, petal_width double, category string");

		source_url.firstN(5).print();

		CsvSourceStreamOp source_stream = new CsvSourceStreamOp()
			.setFilePath("http://archive.ics.uci.edu/ml/machine-learning-databases"
				+ "/iris/iris.data")
			.setSchemaStr("sepal_length double, sepal_width double, "
				+ "petal_length double, petal_width double, category string");

		source_stream.filter("sepal_length < 4.5").print();
		StreamOperator.execute();

		CsvSourceBatchOp wine_url = new CsvSourceBatchOp()
			.setFilePath(LOCAL_DIR + "winequality-white.csv")
			.setSchemaStr("fixedAcidity double,volatileAcidity double,citricAcid double,"
				+ "residualSugar double, chlorides double,freeSulfurDioxide double,"
				+ "totalSulfurDioxide double,density double, pH double,"
				+ "sulphates double,alcohol double,quality double")
			.setFieldDelimiter(";")
			.setIgnoreFirstLine(true);

		wine_url.firstN(5).print();

	}

	static void c_2_1_2() throws Exception {
		HadoopFileSystem hdfs = new HadoopFileSystem(HADOOP_VERSION, HDFS_URI);

		OssFileSystem oss =
			new OssFileSystem(
				OSS_VERSION,
				OSS_END_POINT,
				OSS_BUCKET_NAME,
				OSS_ACCESS_ID,
				OSS_ACCESS_KEY
			);

		FilePath[] filePaths = new FilePath[] {
			new FilePath(LOCAL_DIR + "iris.csv"),
			new FilePath(HDFS_URI + "user/yangxu/alink/data/temp/iris.csv", hdfs),
			new FilePath(OSS_PREFIX_URI + "alink/data/temp/iris.csv", oss)
		};

		for (FilePath filePath : filePaths) {
			new CsvSourceBatchOp()
				.setFilePath(IRIS_HTTP_URL)
				.setSchemaStr(IRIS_SCHEMA_STR)
				.link(
					new CsvSinkBatchOp()
						.setFilePath(filePath)
						.setOverwriteSink(true)
				);
			BatchOperator.execute();

			System.out.println(
				new CsvSourceBatchOp()
					.setFilePath(filePath)
					.setSchemaStr(IRIS_SCHEMA_STR)
					.count()
			);
		}

		for (FilePath filePath : filePaths) {
			new CsvSourceStreamOp()
				.setFilePath(IRIS_HTTP_URL)
				.setSchemaStr(IRIS_SCHEMA_STR)
				.link(
					new CsvSinkStreamOp()
						.setFilePath(filePath)
						.setOverwriteSink(true)
				);
			StreamOperator.execute();

			new CsvSourceStreamOp()
				.setFilePath(filePath)
				.setSchemaStr(IRIS_SCHEMA_STR)
				.filter("sepal_length < 4.5")
				.print();
			StreamOperator.execute();
		}

	}

	static void c_2_2() throws Exception {

		new TsvSourceBatchOp()
			.setFilePath(LOCAL_DIR + "u.data")
			.setSchemaStr("user_id long, item_id long, rating float, ts long")
			.firstN(5)
			.print();

		new TextSourceBatchOp()
			.setFilePath(LOCAL_DIR + "iris.scale")
			.firstN(5)
			.print();

		new LibSvmSourceBatchOp()
			.setFilePath(LOCAL_DIR + "iris.scale")
			.firstN(5)
			.lazyPrint(5, "< read by LibSvmSourceBatchOp >")
			.link(
				new VectorNormalizeBatchOp()
					.setSelectedCol("features")
			)
			.print();

	}

	static void c_2_3_1() throws Exception {
		HadoopFileSystem hdfs = new HadoopFileSystem(HADOOP_VERSION, HDFS_URI);

		OssFileSystem oss =
			new OssFileSystem(
				OSS_VERSION,
				OSS_END_POINT,
				OSS_BUCKET_NAME,
				OSS_ACCESS_ID,
				OSS_ACCESS_KEY
			);

		FilePath[] filePaths = new FilePath[] {
			new FilePath(LOCAL_DIR + "iris.ak"),
			new FilePath(HDFS_URI + "user/yangxu/alink/data/temp/iris.ak", hdfs),
			new FilePath(OSS_PREFIX_URI + "alink/data/temp/iris.ak", oss)
		};

		for (FilePath filePath : filePaths) {
			new CsvSourceBatchOp()
				.setFilePath(IRIS_HTTP_URL)
				.setSchemaStr(IRIS_SCHEMA_STR)
				.link(
					new AkSinkBatchOp()
						.setFilePath(filePath)
						.setOverwriteSink(true)
				);
			BatchOperator.execute();

			System.out.println(
				new AkSourceBatchOp()
					.setFilePath(filePath)
					.count()
			);
		}

		for (FilePath filePath : filePaths) {
			new CsvSourceStreamOp()
				.setFilePath(IRIS_HTTP_URL)
				.setSchemaStr(IRIS_SCHEMA_STR)
				.link(
					new AkSinkStreamOp()
						.setFilePath(filePath)
						.setOverwriteSink(true)
				);
			StreamOperator.execute();

			new AkSourceStreamOp()
				.setFilePath(filePath)
				.filter("sepal_length < 4.5")
				.print();
			StreamOperator.execute();
		}
	}

	static void c_2_4() throws Exception {

		new ParquetSourceBatchOp()
			.setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.parquet")
			.lazyPrintStatistics()
			.print();

		new ParquetSourceStreamOp()
			.setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.parquet")
			.print();
		StreamOperator.execute();

	}

	static void c_2_5_1() throws Exception {

		StreamOperator <?> source = new TsvSourceStreamOp()
			.setFilePath("http://files.grouplens.org/datasets/movielens/ml-100k/u.data")
			.setSchemaStr("user_id long, item_id long, rating float, ts long")
			.udf("ts", "ts", new FromUnixTimestamp());

		source.link(
			new Export2FileSinkStreamOp()
				.setFilePath(LOCAL_DIR + "with_local_time")
				.setWindowTime(5)
				.setOverwriteSink(true)
		);

		source.link(
			new AkSinkStreamOp()
				.setFilePath(LOCAL_DIR + "ratings.ak")
				.setOverwriteSink(true)
		);

		StreamOperator.execute();

		new AkSourceBatchOp()
			.setFilePath(LOCAL_DIR + "with_local_time")
			.lazyPrintStatistics("Statistics for data in the folder 'with_local_time' : ");
		BatchOperator.execute();

	}

	static void c_2_5_2() throws Exception {

		new AkSourceBatchOp()
			.setFilePath(LOCAL_DIR + "ratings.ak")
			.orderBy("ts", 1000000)
			.lazyPrintStatistics("Statistics for data in the file 'ratings.ak' : ")
			.link(
				new AkSinkBatchOp()
					.setFilePath(LOCAL_DIR + "ratings_ordered.ak")
					.setOverwriteSink(true)
			);
		BatchOperator.execute();

		new AkSourceStreamOp()
			.setFilePath(LOCAL_DIR + "ratings_ordered.ak")
			.link(
				new Export2FileSinkStreamOp()
					.setFilePath(LOCAL_DIR + "with_ts_time")
					.setTimeCol("ts")
					.setWindowTime(3600 * 24)
					.setOverwriteSink(true)
			);
		StreamOperator.execute();

		new AkSourceBatchOp()
			.setFilePath(LOCAL_DIR + "with_ts_time")
			.lazyPrintStatistics("Statistics for data in the folder 'with_ts_time' : ");

		new AkSourceBatchOp()
			.setFilePath(LOCAL_DIR + "with_ts_time" + File.separator + "199709210000000")
			.print();

	}

	public static class FromUnixTimestamp extends ScalarFunction {

		public java.sql.Timestamp eval(Long ts) {
			return new java.sql.Timestamp(ts * 1000);
		}

	}

}