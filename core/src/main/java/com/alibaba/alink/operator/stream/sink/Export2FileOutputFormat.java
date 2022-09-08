package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.io.filesystem.AkStream;
import com.alibaba.alink.common.io.filesystem.AkStream.AkWriter.AkCollector;
import com.alibaba.alink.common.io.filesystem.AkUtils.AkMeta;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.filesystem.copy.FileOutputFormat;
import com.alibaba.alink.operator.common.stream.model.ModelStreamUtils;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;

public class Export2FileOutputFormat extends FileOutputFormat <Row> {

	public static final String IN_PROGRESS_FILE_SUFFIX = ".inprogress";

	private final FilePath filePath;
	private final WriteMode writeMode;
	private final int timeColIndex;
	private final int mTableColIndex;
	private final List <Tuple2 <String, SimpleDateFormat>> dataFormats;

	public Export2FileOutputFormat(
		FilePath filePath, WriteMode writeMode,
		List <Tuple2 <String, SimpleDateFormat>> dataFormats, int timeColIndex, int mTableColIndex) {

		super(filePath.getPath(), filePath.getFileSystem());

		this.filePath = filePath;
		this.writeMode = writeMode;
		this.timeColIndex = timeColIndex;
		this.mTableColIndex = mTableColIndex;

		this.dataFormats = dataFormats;

		setWriteMode(writeMode);

		// hack for initial stage of local filesystem.
		if (filePath.getFileSystem().isDistributedFS()) {
			return;
		}

		// check if path exists
		try {
			if (filePath.getFileSystem().exists(filePath.getPath())) {
				// path exists, check write mode
				switch (writeMode) {

					case NO_OVERWRITE:
						// file or directory may not be overwritten
						throw new AkIllegalOperatorParameterException(
							"File or directory already exists. Existing files and directories are not overwritten "
								+ "in "
								+
								WriteMode.NO_OVERWRITE.name() + " mode. Use " + WriteMode.OVERWRITE.name() +
								" mode to overwrite existing files and directories.");

					case OVERWRITE:
						// output path exists. We delete it and all contained files in case of a directory.
						filePath.getFileSystem().delete(filePath.getPath(), true);
						break;

					default:
						throw new AkUnsupportedOperationException("Invalid write mode: " + writeMode);
				}
			}
		} catch (IOException e) {
			throw new AkUnclassifiedErrorException("Error. ", e);
		}
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {

		Path p = this.outputFilePath;
		if (p == null) {
			throw new IOException("The file path is null.");
		}

		final FileSystem fs = filePath.getFileSystem();

		// if this is a local file system, we need to initialize the local output directory here
		if (!fs.isDistributedFS()) {

			if (!fs.initOutPathLocalFS(p, writeMode, true)) {
				// output preparation failed! Cancel task.
				throw new IOException(
					"Output path '" + p + "' could not be initialized. Canceling task...");
			}
		} else {
			// numTasks > 1 || outDirMode == OutputDirectoryMode.ALWAYS

			if (!fs.initOutPathLocalFS(p, writeMode, true)) {
				// output preparation failed! Cancel task.
				throw new IOException(
					"Output directory '" + p + "' could not be created. Canceling task...");
			}
		}
	}

	@Override
	public void writeRecord(Row record) throws IOException {

		Timestamp timestamp = (Timestamp) record.getField(timeColIndex);
		MTable mTable = (MTable) record.getField(mTableColIndex);

		String fileName = ModelStreamUtils.toStringPresentation(timestamp);

		FilePath root = filePath;

		if (dataFormats != null) {

			Path path = root.getPath();

			for (Tuple2 <String, SimpleDateFormat> format : dataFormats) {
				path = new Path(path, String.format("%s=%s", format.f0, format.f1.format(timestamp)));
			}

			root = new FilePath(path, root.getFileSystem());
		}

		FilePath inProgressFilePath = new FilePath(
			new Path(root.getPath(), String.format("%s%s", fileName, IN_PROGRESS_FILE_SUFFIX)),
			root.getFileSystem()
		);

		AkStream akStream = new AkStream(inProgressFilePath, new AkMeta(mTable.getSchemaStr()));

		AkCollector collector = akStream.getWriter().getCollector();

		for (Row row : mTable.getRows()) {
			collector.collect(row);
		}

		collector.close();

		FilePath result = new FilePath(
			new Path(root.getPath(), fileName), root.getFileSystem()
		);

		root.getFileSystem().rename(inProgressFilePath.getPath(), result.getPath());
	}
}
