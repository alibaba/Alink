package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.AkStream;
import com.alibaba.alink.common.io.filesystem.AkStream.AkWriter.AkCollector;
import com.alibaba.alink.common.io.filesystem.AkUtils.AkMeta;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.filesystem.copy.FileOutputFormat;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.stream.model.ModelStreamUtils;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.TumbleTimeWindowStreamOp;
import com.alibaba.alink.params.io.Export2FileSinkParams;

import java.io.IOException;
import java.sql.Timestamp;

/**
 * Sink stream op data to a file system with ak format.
 */
@IoOpAnnotation(name = "export_2_file", ioType = IOType.SinkStream)
@NameCn("流导出到文件")
public final class Export2FileSinkStreamOp extends BaseSinkStreamOp <Export2FileSinkStreamOp>
	implements Export2FileSinkParams <Export2FileSinkStreamOp> {

	private static final long serialVersionUID = -8082608225204145645L;

	public Export2FileSinkStreamOp() {
		this(new Params());
	}

	public Export2FileSinkStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(Export2FileSinkStreamOp.class), params);
	}

	@Override
	public Export2FileSinkStreamOp linkFrom(StreamOperator <?>... inputs) {
		return sinkFrom(checkAndGetFirst(inputs));
	}

	@Override
	public Export2FileSinkStreamOp sinkFrom(StreamOperator <?> in) {

		String timeCol = getTimeCol();

		String[] names = in.getColNames();

		StringBuilder cols = new StringBuilder(names[0]);
		for (int i = 1; i < names.length; i++) {
			cols.append(",").append(names[i]);
		}

		final String windowEndCol = "window_end";
		final String mTableCol = "mt";

		StreamOperator <?> stream;

		if (timeCol == null) {

			timeCol = "ts";

			stream = in
				.select(String.format("LOCALTIMESTAMP AS %s, %s", timeCol, cols))
				.link(
					new TumbleTimeWindowStreamOp()
						.setTimeCol(timeCol)
						.setWindowTime(getWindowTime())
						.setClause(
							String.format("TUMBLE_END() as %s, MTABLE_AGG( %s ) AS %s", windowEndCol, cols, mTableCol)
						)
				);
		} else {
			stream = in
				.link(
					new TumbleTimeWindowStreamOp()
						.setTimeCol(timeCol)
						.setWindowTime(getWindowTime())
						.setClause(
							String.format("TUMBLE_END() as %s, MTABLE_AGG( %s ) AS %s", windowEndCol, cols, mTableCol)
						)
				);
		}

		final int windowEndColIndex = TableUtil.findColIndexWithAssert(stream.getSchema(), windowEndCol);
		final int mTableColIndex = TableUtil.findColIndexWithAssert(stream.getSchema(), mTableCol);

		stream
			.getDataStream()
			.addSink(new OutputFormatSinkFunction <>(
				new Export2FileOutputFormat(
					getFilePath(),
					getOverwriteSink() ? FileSystem.WriteMode.OVERWRITE : FileSystem.WriteMode.NO_OVERWRITE,
					windowEndColIndex,
					mTableColIndex
				)
			))
			.name("export-2-file-sink");

		return this;
	}

	public static class Export2FileOutputFormat extends FileOutputFormat <Row> {

		public static final String IN_PROGRESS_FILE_SUFFIX = ".inprogress";

		private final FilePath filePath;
		private final WriteMode writeMode;
		private final int timeColIndex;
		private final int mTableColIndex;

		public Export2FileOutputFormat(FilePath filePath, WriteMode writeMode, int timeColIndex, int mTableColIndex) {
			super(filePath.getPath(), filePath.getFileSystem());

			this.filePath = filePath;
			this.writeMode = writeMode;
			this.timeColIndex = timeColIndex;
			this.mTableColIndex = mTableColIndex;

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
							throw new RuntimeException(
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
							throw new IllegalArgumentException("Invalid write mode: " + writeMode);
					}
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
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
						"Output path '" + p.toString() + "' could not be initialized. Canceling task...");
				}
			} else {
				// numTasks > 1 || outDirMode == OutputDirectoryMode.ALWAYS

				if (!fs.initOutPathLocalFS(p, writeMode, true)) {
					// output preparation failed! Cancel task.
					throw new IOException(
						"Output directory '" + p.toString() + "' could not be created. Canceling task...");
				}
			}
		}

		@Override
		public void writeRecord(Row record) throws IOException {

			Timestamp timestamp = (Timestamp) record.getField(timeColIndex);
			MTable mTable = (MTable) record.getField(mTableColIndex);

			String fileName = ModelStreamUtils.toStringPresentation(timestamp);

			FilePath inProgressFilePath = new FilePath(
				new Path(filePath.getPath(), String.format("%s%s", fileName, IN_PROGRESS_FILE_SUFFIX)), filePath.getFileSystem()
			);

			AkStream akStream = new AkStream(inProgressFilePath, new AkMeta(mTable.getSchemaStr()));

			AkCollector collector = akStream.getWriter().getCollector();

			for (Row row : mTable.getRows()) {
				collector.collect(row);
			}

			collector.close();

			FilePath result = new FilePath(
				new Path(filePath.getPath(), fileName), filePath.getFileSystem()
			);

			filePath.getFileSystem().rename(inProgressFilePath.getPath(), result.getPath());
		}
	}
}
