package com.alibaba.alink.common.dl.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.DownloadUtils;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.flink.ml.util.IpHostUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DataSetDiskDownloader implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(DataSetDiskDownloader.class);

	private static final String MARK_FILENAME = ".mark";

	public static void unzipUserFileFromDisk(String[] fileList, String targetDir) throws IOException {
		String zipFilePath = null;
		for (String fileName : fileList) {
			if (new File(fileName).exists()) {
				zipFilePath = fileName;
				break;
			}
		}
		if (null != zipFilePath) {
			ArchivesUtils.decompressFile(new File(zipFilePath), new File(targetDir));
		}
	}

	private static BatchOperator handleEmptyFile() {
		ExecutionEnvironment env = MLEnvironmentFactory.getDefault().getExecutionEnvironment();
		DataSet <Row> empty = env.fromElements(0).mapPartition(new MapPartitionFunction <Integer, Row>() {
			@Override
			public void mapPartition(Iterable <Integer> values, Collector <Row> out) throws Exception {
				values.forEach(t -> {
					// doing nothing, just to avoid a bug in Blink.
				});
			}
		});
		BatchOperator modelSource = BatchOperator.fromTable(
			DataSetConversionUtil.toTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,
				empty,
				new String[] {"targetPath"},
				new TypeInformation[] {TypeInformation.of(String.class)}));
		return modelSource;
	}

	/**
	 * task 0 downloads the file and broadcast to all other workers.
	 *
	 * @return barrier as the barrier for other dataset operations. Row(String Path)
	 */
	public static BatchOperator downloadUserFile(String uri) {
		ExecutionEnvironment env = MLEnvironmentFactory.getDefault().getExecutionEnvironment();
		if (null == uri || uri.length() == 0) {
			return handleEmptyFile();
		}
		// expand data parallelism by partitionBy
		DataSet <Integer> dataSetWithMaxParallelism = env.fromElements(Tuple2.of(1, 1))
			.partitionCustom(new Partitioner <Integer>() {
				@Override
				public int partition(Integer key, int numPartitions) {
					return key % numPartitions;
				}
			}, 0).map(new MapFunction <Tuple2 <Integer, Integer>, Integer>() {
				@Override
				public Integer map(Tuple2 <Integer, Integer> value) throws Exception {
					return value.f1;
				}
			});
		DataSet <Row> filePaths = dataSetWithMaxParallelism
			.mapPartition(new RichMapPartitionFunction <Integer, Tuple2 <Integer, byte[]>>() {

				String targetFileName;
				String targetDir;

				@Override
				public void open(Configuration configuration) throws Exception {
					// delete the file in task zero.
					int taskId = getRuntimeContext().getIndexOfThisSubtask();
					if (taskId == 0) {
						targetFileName = uri.contains("\\")
							? uri.substring(uri.lastIndexOf('\\') + 1)
							: uri.substring(uri.lastIndexOf('/') + 1);
						targetDir = PythonFileUtils.createTempDir("temp_user_files_").toString();
						Path localPath = Paths.get(targetDir, targetFileName).toAbsolutePath();
						File file = localPath.toFile();
						if (file.exists()) {
							file.delete();
						}
					}
				}

				@Override
				public void mapPartition(Iterable <Integer> values, Collector <Tuple2 <Integer, byte[]>> out)
					throws Exception {
					int taskId = getRuntimeContext().getIndexOfThisSubtask();
					int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
					if (taskId == 0) {
						DownloadUtils.resumableDownloadHttpFile(uri, targetDir, targetFileName);
						// read from local disk and send to other workers
						Path localPath = Paths.get(targetDir, targetFileName).toAbsolutePath();
						File fileOnDisk = localPath.toFile();
						FileInputStream fis = new FileInputStream(fileOnDisk);

						int read;
						final int buffSize = 64 * 1024;    // 64KB
						byte[] buffer = new byte[buffSize];
						while ((read = fis.read(buffer, 0, buffSize)) != -1) {
							byte[] toSend = new byte[read];
							System.arraycopy(buffer, 0, toSend, 0, read);
							for (int idx = 0; idx < numTasks; idx++) {
								out.collect(Tuple2.of(idx, toSend));
							}
						}
						LOG.info("Downloading on TM with taskId: " + taskId + " ip: " + IpHostUtil.getIpAddress());
						fis.close();
						// delete the file.
					} else {
						LOG.info("No downloading on TM with taskId: " + taskId + " ip: " + IpHostUtil.getIpAddress());
					}
				}

			})
			.partitionCustom(new Partitioner <Integer>() {
				@Override
				public int partition(Integer key, int numPartitions) {
					return key;
				}
			}, 0)
			.mapPartition(new RichMapPartitionFunction <Tuple2 <Integer, byte[]>, Row>() {

				String targetFileName;
				String targetDir;

				@Override
				public void open(Configuration configuration) throws Exception {
					targetFileName = uri.contains("\\") ? uri.substring(uri.lastIndexOf('\\') + 1):
							uri.substring(uri.lastIndexOf('/') + 1);
					targetDir = PythonFileUtils.createTempDir("temp_user_files_").toString();
					Path localPath = Paths.get(targetDir, targetFileName).toAbsolutePath();
					File file = localPath.toFile();
					if (file.exists()) {
						file.delete();
					}

				}

				@Override
				public void mapPartition(Iterable <Tuple2 <Integer, byte[]>> values, Collector <Row> out)
					throws Exception {
					int taskId = getRuntimeContext().getIndexOfThisSubtask();
					// write to disk
					Path localPath = Paths.get(targetDir, targetFileName).toAbsolutePath();
					File outputFile = localPath.toFile();
					FileOutputStream fos = new FileOutputStream(outputFile, true);
					for (Tuple2 <Integer, byte[]> val : values) {
						fos.write(val.f1, 0, val.f1.length);
					}
					fos.close();
					LOG.info("Write to disk on TM with taskId: " + taskId + " ip: " + IpHostUtil.getIpAddress());
					Row row = new Row(1);
					row.setField(0, targetDir + File.separator + targetFileName);
					out.collect(row);
				}
			});
		BatchOperator modelSource = BatchOperator.fromTable(
			DataSetConversionUtil.toTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,
				filePaths,
				new String[] {"targetPath"},
				new TypeInformation[] {TypeInformation.of(String.class)}));
		return modelSource;
	}

	static class DownloadFilesMapPartitionFunction
		extends RichMapPartitionFunction <Integer, Tuple2 <Integer, byte[]>> {
		private final List <String> paths;

		public DownloadFilesMapPartitionFunction(List <String> paths) {
			this.paths = paths;
		}

		@Override
		public void mapPartition(Iterable <Integer> values, Collector <Tuple2 <Integer, byte[]>> out)
			throws Exception {
			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
			if (taskId == 0) {
				String targetDir = PythonFileUtils.createTempDir(String.format("downloaded_files_%s_", taskId)).toString();
				for (String path : paths) {
					if (FileDownloadUtils.isLocalPath(path)) {
						continue;
					}

					String fn = FileDownloadUtils.downloadFileToDirectory(path, new File(targetDir));
					//String fn = PythonFileUtils.getFileName(path);
					//resumableDownloadFileByPieces(path, targetDir, fn);
					File fileOnDisk = new File(targetDir, fn);
					FileInputStream fis = new FileInputStream(fileOnDisk);

					int read;
					final int buffSize = 64 * 1024;    // 64KB
					byte[] buffer = new byte[buffSize];
					while ((read = fis.read(buffer, 0, buffSize)) != -1) {
						if (read > 0) {
							byte[] toSend = new byte[read];
							System.arraycopy(buffer, 0, toSend, 0, read);
							for (int idx = 0; idx < numTasks; idx++) {
								out.collect(Tuple2.of(idx, toSend));
							}
						}
					}
					for (int idx = 0; idx < numTasks; idx++) {
						out.collect(Tuple2.of(idx, new byte[0]));    // flag for end of file
					}
					LOG.info("Downloading on TM with taskId: " + taskId + " ip: " + IpHostUtil.getIpAddress());
					fis.close();
				}
			} else {
				LOG.info("No downloading on TM with taskId: " + taskId + " ip: " + IpHostUtil.getIpAddress());
			}
		}
	}

	static class ExtractRenameFilesMapPartitionFunction
		extends RichMapPartitionFunction <Tuple2 <Integer, byte[]>, Row> {

		private final List <String> paths;
		private final Map <String, String> renameMap;

		public ExtractRenameFilesMapPartitionFunction(List <String> paths, Map <String, String> renameMap) {
			this.paths = paths;
			this.renameMap = renameMap;
		}

		@Override
		public void mapPartition(Iterable <Tuple2 <Integer, byte[]>> values, Collector <Row> out) throws Exception {
			Iterator <Tuple2 <Integer, byte[]>> iterator = values.iterator();

			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			String targetDir = PythonFileUtils.createTempDir(String.format("work_dir_%s_", taskId)).toString();
			// The order of values are supposed to be unchanged.
			for (String path : paths) {
				boolean isCompressed = PythonFileUtils.isCompressedFile(path);
				String rename = renameMap.get(path);
				String fn = (!isCompressed) && (rename != null)
					? rename
					: PythonFileUtils.getFileName(path);
				File outputFile = new File(targetDir, fn);

				if (FileDownloadUtils.isLocalPath(path)) {
					FileDownloadUtils.downloadFile(path, outputFile);
				} else {
					FileOutputStream fos = new FileOutputStream(outputFile);
					while (iterator.hasNext()) {
						Tuple2 <Integer, byte[]> next = iterator.next();
						byte[] data = next.f1;
						if (data.length > 0) {
							fos.write(data, 0, data.length);
						} else {
							break;
						}
					}
					fos.close();
				}

				if (isCompressed) {
					File decompressedDir = rename != null
						? new File(targetDir, rename)
						: new File(targetDir);
					PythonFileUtils.ensureDirectoryExists(decompressedDir);
					ArchivesUtils.decompressFile(outputFile, decompressedDir);
				}
			}
			Row row = Row.of(targetDir);
			out.collect(row);
		}
	}

	/**
	 * Worker with task 0 downloads all files from `paths` and broadcast them to all other workers. All files are saved
	 * into a local directory.
	 * <p>
	 * Then, normal files are renamed according to `renameMap`. Compressed files are decompressed to the local
	 * directory, or saved to a sub-directory in the local directory if specified in `renameMap`.
	 *
	 * @param paths
	 * @param renameMap
	 * @return
	 */
	public static BatchOperator <?> downloadFilesWithRename(Long mlEnvId, List <String> paths,
															Map <String, String> renameMap) {
		ExecutionEnvironment env = MLEnvironmentFactory.getDefault().getExecutionEnvironment();

		String outputCol = "targetPath";
		TypeInformation <?> outputColType = Types.STRING;

		if (paths.size() == 0) {
			return handleEmptyFile();
		}

		// expand data parallelism by partitionBy
		DataSet <Integer> dataSetWithMaxParallelism = env.fromElements(Tuple2.of(1, 1))
			.partitionCustom(new Partitioner <Integer>() {
				@Override
				public int partition(Integer key, int numPartitions) {
					return key % numPartitions;
				}
			}, 0).map(new MapFunction <Tuple2 <Integer, Integer>, Integer>() {
				@Override
				public Integer map(Tuple2 <Integer, Integer> value) throws Exception {
					return value.f1;
				}
			});
		DataSet <Row> filePaths = dataSetWithMaxParallelism
			.mapPartition(new DownloadFilesMapPartitionFunction(paths))
			.partitionCustom(new Partitioner <Integer>() {
				@Override
				public int partition(Integer key, int numPartitions) {
					return key;
				}
			}, 0)
			.mapPartition(new ExtractRenameFilesMapPartitionFunction(paths, renameMap));

		BatchOperator <?> modelSource = BatchOperator.fromTable(
			DataSetConversionUtil.toTable(mlEnvId,
				filePaths,
				new String[] {"targetPath"},
				new TypeInformation[] {TypeInformation.of(String.class)}));
		return modelSource;
	}

	static synchronized boolean markPath(File file) {
		try {
			return file.createNewFile();
		} catch (IOException e) {
			return false;
		}
	}

	public static synchronized String getDownloadPath(String[] downloadPathCandidates) {
		String downloadPath = null;
		for (String candidate : downloadPathCandidates) {
			if (new File(candidate).exists()) {
				File markFile = new File(candidate, MARK_FILENAME);
				if (markPath(markFile)) {
					downloadPath = candidate;
					break;
				}
			}
		}
		if (downloadPath == null) {
			throw new AkUnclassifiedErrorException("Cannot get download path from candidates: " +
				Arrays.toString(downloadPathCandidates));
		}
		return downloadPath;
	}

	public static void moveFilesToWorkDir(String[] downloadPathCandidates, File targetDir) throws IOException {
		String downloadPath = getDownloadPath(downloadPathCandidates);
		File[] files = new File(downloadPath).listFiles();
		assert files != null;
		for (File file : files) {
			String filename = file.getName();
			Files.createSymbolicLink(new File(targetDir, filename).toPath(), file.toPath());
		}
	}
}
