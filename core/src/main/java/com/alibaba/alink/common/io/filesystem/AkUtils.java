package com.alibaba.alink.common.io.filesystem;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.filesystem.copy.FileInputFormat;
import com.alibaba.alink.common.io.filesystem.copy.FileOutputFormat;
import com.alibaba.alink.common.utils.JsonConverter;
import org.apache.commons.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class AkUtils {

	public static final String META_FILE = "alink_meta.json";
	public static final String DATA_FILE = "data";

	public static class AkMeta implements Serializable {
		private static final long serialVersionUID = 1L;

		public String fileFormat = "binary";
		public String schemaStr;
		public int numFiles = 1;
		public String version = "v0.1";

		public AkMeta() {
		}

		public AkMeta(String schemaStr) {
			this.schemaStr = schemaStr;
		}
	}

	public static boolean isAkFile(FilePath filePath) throws IOException {
		FileStatus fileStatus = filePath.getFileSystem().getFileStatus(filePath.getPath());

		if (fileStatus.isDir()) {
			return getFromFolder(filePath, AkUtils::tryOpenAkFile);
		} else {
			return tryOpenAkFile(filePath);
		}
	}

	private static boolean tryOpenAkFile(FilePath filePath) throws IOException {
		try (FSDataInputStream fsDataInputStream = filePath.getFileSystem().open(filePath.getPathStr())) {
			try (ZipInputStream zis = new ZipInputStream(fsDataInputStream)) {
				return zis.getNextEntry() != null;
			} catch (Exception ex) {
				return false;
			}
		}
	}

	public static AkMeta getMetaFromPath(FilePath filePath) throws IOException {
		FileStatus fileStatus = filePath.getFileSystem().getFileStatus(filePath.getPath());

		if (fileStatus.isDir()) {
			return getFromFolder(filePath, AkUtils::getMetaFromAkFile);
		} else {
			return getMetaFromAkFile(filePath);
		}
	}

	public static AkMeta getMetaFromAkFile(FilePath filePath) throws IOException {
		return readMetaFromFile(filePath);
	}

	private static AkMeta readMetaFromFile(FilePath filePath) throws IOException {

		AkMeta meta = null;
		ZipEntry entry;
		try (ZipInputStream zis = new ZipInputStream(
			new BufferedInputStream(filePath.getFileSystem().open(filePath.getPathStr())))) {
			while ((entry = zis.getNextEntry()) != null) {
				if (entry.getName().equalsIgnoreCase(AkUtils.META_FILE)) {
					meta = JsonConverter.fromJson(
						IOUtils.toString(zis, StandardCharsets.UTF_8),
						AkMeta.class
					);
					break;
				}
			}
		}

		return meta;
	}

	private interface FileProcFunction<T, R> {
		R apply(T t) throws IOException;
	}

	private static <T> T getFromFolder(FilePath filePath, FileProcFunction <FilePath, T> fileProc) throws IOException {
		Path fileNamed1 = new Path(filePath.getPath(), "1");

		if (filePath.getFileSystem().exists(fileNamed1)
			&& !filePath.getFileSystem().getFileStatus(fileNamed1).isDir()) {

			// get file named taskId + 1
			return fileProc.apply(new FilePath(fileNamed1, filePath.getFileSystem()));
		} else {
			List <Path> files = filePath.getFileSystem().listDirectories(filePath.getPath());

			if (files.isEmpty()) {
				throw new IOException(
					"Folder is empty. Could not determined schema of op. folder: " + filePath.getPathStr()
				);
			} else {
				return fileProc.apply(new FilePath(files.get(0), filePath.getFileSystem()));
			}
		}
	}

	public static class AkInputFormat extends FileInputFormat <Row> {
		private static final long serialVersionUID = -2602228246743287382L;

		private final AkMeta meta;

		private transient boolean isInactiveSplit;
		private transient AkStream.AkReader reader;
		private transient AkStream.AkReader.AkReadIterator readIterator;

		public AkInputFormat(FilePath filePath, AkMeta meta) {
			super(filePath.getPath(), filePath.getFileSystem());
			this.meta = meta;
		}

		@Override
		public void open(FileInputSplit fileSplit) throws IOException {
			isInactiveSplit = fileSplit.getStart() > 0;
			if (isInactiveSplit) {
				return;
			}

			super.open(fileSplit);

			reader = new AkStream(meta)
				.getReader(stream);

			readIterator = reader.iterator();
		}

		@Override
		public void close() throws IOException {
			if (reader != null) {
				reader.close();
				reader = null;
			}
			super.close();
		}

		@Override
		public boolean reachedEnd() throws IOException {
			if (isInactiveSplit) {
				return true;
			}
			return !readIterator.hasNext();
		}

		@Override
		public Row nextRecord(Row reuse) throws IOException {
			return readIterator.next();
		}
	}

	public static class AkOutputFormat extends FileOutputFormat <Row> {
		private static final long serialVersionUID = 7495725429804084574L;
		private AkMeta meta;

		private transient AkStream.AkWriter.AkCollector collector;

		public AkOutputFormat(FilePath filePath, AkMeta meta, WriteMode writeMode) {
			super(filePath.getPath(), filePath.getFileSystem());

			this.meta = meta;

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
			super.open(taskNumber, numTasks);
			collector = new AkStream(meta)
				.getWriter(stream)
				.getCollector();
		}

		@Override
		public void close() throws IOException {
			if (collector != null) {
				collector.close();
				collector = null;
			}
			super.close();
		}

		@Override
		public void writeRecord(Row t) {
			collector.collect(t);
		}
	}
}
