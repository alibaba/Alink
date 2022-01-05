package com.alibaba.alink.common.dl.utils;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataSetDiskDownloaderTest {

	@Test
	public void test() throws Exception {
		PythonFileUtils.DELETE_TEMP_FILES_WHEN_EXIT = false;
		String input
			= "http://pai-algo-public.oss-cn-hangzhou-zmf.aliyuncs.com/alink_congzhou/DataSetDiskDownloaderTest.tar"
			+ ".gz";
		int savedParallelism = MLEnvironmentFactory.getDefault().getExecutionEnvironment().getParallelism();
		BatchOperator.setParallelism(3);
		ExecutionEnvironment env = MLEnvironmentFactory.getDefault().getExecutionEnvironment();

		BatchOperator paths = DataSetDiskDownloader.downloadUserFile(input);

		env.fromElements(Tuple2.of(0, 0)).partitionCustom(new Partitioner <Integer>() {
			@Override
			public int partition(Integer key, int numPartitions) {
				return key;
			}
		}, 0).mapPartition(
			new RichMapPartitionFunction <Tuple2 <Integer, Integer>, Integer>() {
				@Override
				public void mapPartition(Iterable <Tuple2 <Integer, Integer>> values, Collector <Integer> out)
					throws Exception {
					List <Row> bcpaths = getRuntimeContext().getBroadcastVariable("paths");
					String[] filePaths = new String[bcpaths.size()];
					for (int i = 0; i < filePaths.length; i++) {
						filePaths[i] = (String) bcpaths.get(i).getField(0);
					}
					DataSetDiskDownloader.unzipUserFileFromDisk(filePaths, PythonFileUtils.createTempDir("temp_").toString());
				}
			}).withBroadcastSet(paths.getDataSet(), "paths").print();
		BatchOperator.setParallelism(savedParallelism);
	}

	@Test
	public void testDownloadFilesWithRename() throws Exception {
		PythonFileUtils.DELETE_TEMP_FILES_WHEN_EXIT = false;
		int savedParallelism = MLEnvironmentFactory.getDefault().getExecutionEnvironment().getParallelism();
		BatchOperator.setParallelism(3);
		List<String> paths = Arrays.asList(
			"http://pai-algo-public.oss-cn-hangzhou-zmf.aliyuncs.com/alink_congzhou/DataSetDiskDownloaderTest.tar.gz"
		);
		Map <String, String> renameMap = new HashMap<>();
		renameMap.put("http://pai-algo-public.oss-cn-hangzhou-zmf.aliyuncs.com/alink_congzhou/DataSetDiskDownloaderTest.tar.gz", "rename");

		BatchOperator<?> downloadPaths = DataSetDiskDownloader.downloadFilesWithRename
			(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, paths, renameMap);
		List <Row> rows = downloadPaths.collect();
		for (Row row : rows) {
			String path = (String) row.getField(0);
			File targetDir = new File(path);
			assert targetDir.exists();
			assert new File(targetDir, "DataSetDiskDownloaderTest.tar.gz").exists();
			assert new File(targetDir, "rename").exists();
			assert new File(targetDir, "rename/DataSetDiskDownloaderTest.txt").exists();
		}
		BatchOperator.setParallelism(savedParallelism);
	}
}
