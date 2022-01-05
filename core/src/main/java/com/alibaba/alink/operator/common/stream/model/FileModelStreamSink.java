package com.alibaba.alink.operator.common.stream.model;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.filesystem.AkStream;
import com.alibaba.alink.common.io.filesystem.AkStream.AkWriter.AkCollector;
import com.alibaba.alink.common.io.filesystem.AkUtils.AkMeta;
import com.alibaba.alink.common.io.filesystem.BaseFileSystem;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.utils.JsonConverter;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Structure of model stream
 * <p>root
 * - conf - id0 - id1
 */
public class FileModelStreamSink implements Serializable {
	private final FilePath filePath;
	private final String schemaStr;

	public static final String MODEL_CONF = "conf";

	private transient AkCollector collector;

	public FileModelStreamSink(FilePath filePath, String schemaStr) {
		this.filePath = filePath;
		this.schemaStr = schemaStr;
	}

	public void initializeGlobal() throws IOException {

		BaseFileSystem <?> fileSystem = filePath.getFileSystem();

		// check and create conf dir.
		Path confDirPath = new Path(filePath.getPath(), MODEL_CONF);

		if (fileSystem.exists(confDirPath)) {
			if (!fileSystem.getFileStatus(confDirPath).isDir()) {
				throw new IllegalStateException("Conf dir of file model is exists and it it not a directory.");
			}
		} else {
			fileSystem.mkdirs(confDirPath);
		}
	}

	public void open(Timestamp modelId, int subId) throws IOException {
		BaseFileSystem <?> fileSystem = filePath.getFileSystem();
		Path confDirPath = new Path(filePath.getPath(), MODEL_CONF);
		Path fileInProgress = new Path(confDirPath,
			String.format("%s_%d", ModelStreamUtils.toStringPresentation(modelId), subId));

		collector = new AkStream(
			new FilePath(fileInProgress, fileSystem),
			new AkMeta(schemaStr)
		).getWriter().getCollector();
	}

	public void collect(Row row) {
		collector.collect(row);
	}

	public void close() {
		if (collector != null) {
			collector.close();
		}
	}

	public void finalizeGlobal(Timestamp modelId, long numRows, int numFiles, int numKeepModel) throws IOException {
		List <Integer> filesId = new ArrayList <>();

		for (int i = 0; i < numFiles; ++i) {
			filesId.add(i);
		}

		finalizeGlobal(modelId, numRows, filesId, numKeepModel);
	}

	public void finalizeGlobal(Timestamp modelId, long numRows, List <Integer> filesId, int numKeepModel)
		throws IOException {

		BaseFileSystem <?> fileSystem = filePath.getFileSystem();

		// construct model folder
		Path confDirPath = new Path(filePath.getPath(), MODEL_CONF);

		Path modelPath = new Path(confDirPath, ModelStreamUtils.toStringPresentation(modelId));

		if (fileSystem.exists(modelPath)) {
			throw new IOException(String.format("ModelPath: %s has existed.", modelPath));
		} else {
			fileSystem.mkdirs(modelPath);
		}

		filesId.sort(Integer::compareTo);

		for (int i = 0; i < filesId.size(); ++i) {
			Path subInProgressModelFilePath = new Path(confDirPath,
				String.format("%s_%d", ModelStreamUtils.toStringPresentation(modelId), filesId.get(i)));
			Path subToCommitModelFilePath = new Path(modelPath, String.valueOf(i));

			if (!fileSystem.rename(subInProgressModelFilePath, subToCommitModelFilePath)) {
				throw new IOException(
					String.format(
						"Submit sub-model %s to %s failed. Maybe folder %s exists.",
						subInProgressModelFilePath,
						subToCommitModelFilePath,
						subToCommitModelFilePath
					)
				);
			}
		}

		// if done, write redo log.
		Path logPath = new Path(confDirPath,
			String.format("%s.log", ModelStreamUtils.toStringPresentation(modelId)));

		try (FSDataOutputStream outputStream = fileSystem.create(logPath, WriteMode.OVERWRITE)) {
			outputStream.write(JsonConverter.toJson(new ModelStreamMeta(numRows, filesId.size())).getBytes());
		} catch (Exception ex) {
			// if write fail, delete the redo log to make the model invalid.
			fileSystem.delete(logPath, false);
			throw ex;
		}

		// if done, do commit.
		Path finalModelPath = new Path(filePath.getPath(), ModelStreamUtils.toStringPresentation(modelId));
		if (!fileSystem.rename(modelPath, finalModelPath)) {
			throw new IOException(
				String.format(
					"Submit model %s to %s failed. Maybe folder %s exists.",
					modelPath,
					finalModelPath,
					finalModelPath
				)
			);
		}

		// if done, do clean up
		cleanUp(filePath, numKeepModel);
	}

	private static void cleanUp(FilePath filePath, int numKeepModel) throws IOException {

		if (numKeepModel < 0) {
			return;
		}

		List <Timestamp> models = ModelStreamUtils.listModels(filePath);

		models.sort(Timestamp::compareTo);

		BaseFileSystem <?> fileSystem = filePath.getFileSystem();
		Path confFolder = new Path(filePath.getPath(), MODEL_CONF);

		for (int i = 0; i < models.size() - numKeepModel; ++i) {
			// do remove

			// remove model
			fileSystem.delete(new Path(filePath.getPath(), ModelStreamUtils.toStringPresentation(models.get(i))),
				true);

			// remove log
			fileSystem.delete(
				new Path(confFolder, String.format("%s.log", ModelStreamUtils.toStringPresentation(models.get(i)))),
				false);
		}
	}
}
