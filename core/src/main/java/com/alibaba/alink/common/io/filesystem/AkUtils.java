package com.alibaba.alink.common.io.filesystem;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkParseErrorException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.io.filesystem.AkStream.AkReader;
import com.alibaba.alink.common.io.filesystem.AkStream.AkReader.AkReadIterator;
import com.alibaba.alink.common.io.filesystem.copy.FileInputFormat;
import com.alibaba.alink.common.io.filesystem.copy.FileOutputFormat;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import com.alibaba.alink.operator.local.sql.WhereLocalOp;
import com.alibaba.alink.operator.stream.sink.Export2FileOutputFormat;
import org.apache.commons.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class AkUtils {

	public static final String META_FILE = "alink_meta.json";
	public static final String DATA_FILE = "data";
	public static final String COLUMN_SPLIT_TAG = "=";

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

	public static Tuple2 <TableSchema, List <Row>> readFromPath(FilePath filePath) throws Exception {
		return readFromPath(filePath, null);
	}

	public static Tuple2 <TableSchema, List <Row>> readFromPath(
		FilePath filePath, FilterFunction <Row> filterFunction) throws Exception {

		FileForEachReaderIterable reader = new FileForEachReaderIterable();

		getFromFolderForEach(filePath, reader);

		List <Row> content = new ArrayList <>();

		if (filterFunction == null) {
			for (Row row : reader) {
				content.add(row);
			}
		} else {
			for (Row row : reader) {
				if (filterFunction.filter(row)) {
					content.add(row);
				}
			}
		}

		return Tuple2.of(reader.getSchema(), content);
	}

	public interface FileProcFunction<T, R> {
		R apply(T t) throws IOException;
	}

	public static class FileForEachReaderIterable implements FileProcFunction <FilePath, Boolean>, Iterable <Row> {
		private final List <FilePath> files = new ArrayList <>();
		private TableSchema schema;

		@Override
		public Boolean apply(FilePath filePath) throws IOException {
			boolean fileExists = filePath.getFileSystem().exists(filePath.getPath());

			if (!fileExists) {
				throw new AkIllegalArgumentException("Could not find the file: " + filePath.getPathStr());
			}

			files.add(filePath);

			return true;
		}

		public TableSchema getSchema() {
			return schema;
		}

		private class ContentIterator implements Iterator <Row> {

			private transient int cursor = 0;
			private transient AkReader akReader;
			private transient AkReadIterator akIterator;

			private void clearState() {
				try {
					if (akReader != null) {
						akReader.close();
					}
				} catch (IOException e) {
					// pass
				} finally {
					akReader = null;
					akIterator = null;
				}
			}

			@Override
			public boolean hasNext() {

				AkPreconditions.checkState(
					akReader == null && akIterator == null || akReader != null && akIterator != null
				);

				while (akIterator == null || !akIterator.hasNext()) {

					if (cursor >= files.size()) {

						clearState();

						return false;
					}

					try {

						if (akReader != null) {
							akReader.close();
						}

						AkStream akStream = new AkStream(files.get(cursor++));

						akReader = akStream.getReader();

						schema = TableUtil.schemaStr2Schema(akStream.getAkMeta().schemaStr);

						akIterator = akReader.iterator();
					} catch (IOException e) {

						clearState();

						throw new AkIllegalDataException("Error. ", e);
					}
				}

				return true;
			}

			@Override
			public Row next() {
				return akIterator.next();
			}
		}

		@Override
		public Iterator <Row> iterator() {
			return new ContentIterator();
		}
	}

	private static <T> T getFromFolder(FilePath filePath, FileProcFunction <FilePath, T> fileProc) throws IOException {

		if (filePath.getFileSystem().exists(filePath.getPath())
			&& !filePath.getFileSystem().getFileStatus(filePath.getPath()).isDir()) {

			return fileProc.apply(filePath);
		} else {
			FileStatus[] files = filePath.getFileSystem().listStatus(filePath.getPath());

			for (FileStatus status : files) {
				T t = getFromFolder(
					new FilePath(status.getPath(), filePath.getFileSystem()),
					fileProc
				);

				if (t != null) {
					return t;
				}
			}
		}

		return null;
	}

	/**
	 * The return value of fileReader indicate that the loop will continue or not.
	 */
	public static void getFromFolderForEach(FilePath filePath, FileProcFunction <FilePath, Boolean> fileReader)
		throws IOException {

		if (filePath.getFileSystem().exists(filePath.getPath())
			&& !filePath.getFileSystem().getFileStatus(filePath.getPath()).isDir()) {

			// get file named taskId + 1
			fileReader.apply(new FilePath(filePath.getPath(), filePath.getFileSystem()));
		} else {
			List <Path> files = filePath.getFileSystem().listFiles(filePath.getPath());

			for (Path path : files) {
				if (!fileReader.apply(new FilePath(path, filePath.getFileSystem()))) {
					break;
				}
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

			setFilesFilter(new FilePathFilter() {
				@Override
				public boolean filterPath(Path filePath) {
					return filePath.getPath().endsWith(Export2FileOutputFormat.IN_PROGRESS_FILE_SUFFIX);
				}
			});

			setNestedFileEnumeration(true);
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

		private final FilePath filePath;

		private final WriteMode writeMode;

		private transient AkStream.AkWriter.AkCollector collector;

		public AkOutputFormat(FilePath filePath, AkMeta meta, WriteMode writeMode) {
			super(filePath.getPath(), filePath.getFileSystem());

			this.meta = meta;

			setWriteMode(writeMode);

			this.filePath = filePath;

			this.writeMode = writeMode;
		}

		@Override
		public void open(int taskNumber, int numTasks) throws IOException {
			super.open(taskNumber, numTasks);

			collector = new AkStream(meta)
				.getWriter(stream)
				.getCollector();
		}

		@Override
		public void initializeGlobal(int parallelism) throws IOException {

			if (!filePath.getFileSystem().isDistributedFS() && filePath.getFileSystem().exists(filePath.getPath())) {
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

			super.initializeGlobal(parallelism);
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

	public static List <Row> listPartitions(FilePath filePath, int numCols) throws IOException {

		int currentDepth = filePath.getPathStr().split(Path.SEPARATOR).length;
		BaseFileSystem <?> baseFileSystem = filePath.getFileSystem();
		Path rootPath = filePath.getPath();
		List <String> pathDirectories = new ArrayList <>();

		getRecursionDirectories(baseFileSystem, rootPath, pathDirectories);

		if (pathDirectories.size() == 0) {
			throw new AkIllegalOperatorParameterException(String.format("no data in path %s", rootPath.getPath()));
		}

		List <Row> rows = new ArrayList <>(pathDirectories.size());
		for (String pathDirectory : pathDirectories) {
			String[] dirPaths = pathDirectory.split(Path.SEPARATOR);
			Row row = new Row(numCols);
			for (int j = 0; j < numCols && j + currentDepth < dirPaths.length; j++) {
				String[] columnValues = splitPath(dirPaths[currentDepth + j]);
				row.setField(j, columnValues[1]);
			}
			rows.add(row);
		}

		return rows;
	}

	public static LocalOperator <?> selectPartitionLocalOp(
		Long mlEnvId, FilePath filePath, String pattern) throws IOException {

		return selectPartitionLocalOp(filePath, pattern, null);
	}

	public static LocalOperator <?> selectPartitionLocalOp(
		FilePath filePath, String pattern, String[] colNames) throws IOException {

		if (colNames == null) {
			colNames = getPartitionColumns(filePath);
		}
		final int numCols = colNames.length;
		final TypeInformation <?>[] colTypes = new TypeInformation <?>[colNames.length];
		for (int i = 0; i < colNames.length; ++i) {
			colTypes[i] = AlinkTypes.STRING;
		}
		final TableSchema schema = new TableSchema(colNames, colTypes);

		return new MemSourceLocalOp(listPartitions(filePath, numCols), schema)
			.link(
				new WhereLocalOp()
					.setClause(transformPattern(pattern, colNames))
			);
	}

	public static void getRecursionDirectories(BaseFileSystem <?> baseFileSystem, Path p, List <String> pathNames)
		throws IOException {
		List <Path> paths = baseFileSystem.listDirectories(p);
		if (paths.size() == 0) {
			pathNames.add(p.getPath());
			return;
		}
		for (Path path : paths) {
			getRecursionDirectories(baseFileSystem, path, pathNames);
		}
	}

	public static String[] getPartitionColumns(FilePath filePath) throws IOException {
		BaseFileSystem <?> baseFileSystem = filePath.getFileSystem();
		List <String> columns = new ArrayList <>();
		Path currentPath = filePath.getPath();
		while (true) {
			List <Path> subdirs = baseFileSystem.listDirectories(currentPath);
			if (subdirs.size() == 0) {
				break;
			}
			currentPath = subdirs.get(0);
			String[] splitValues = splitPath(currentPath.getName());
			columns.add(splitValues[0]);
		}
		return columns.toArray(new String[0]);
	}

	public static String[] splitPath(String dirname) {
		String[] splits = dirname.split(COLUMN_SPLIT_TAG);
		if (splits.length != 2) {
			throw new AkParseErrorException(String.format("invalid directory name %s", dirname));
		}
		return splits;
	}

	public static String transformPattern(String pattern, String[] colNames) {
		HashSet <String> columnSet = new HashSet <>(Arrays.asList(colNames));

		StringBuilder buffer = new StringBuilder();
		StringBuilder tempBuffer = new StringBuilder();
		for (int i = 0; i < pattern.length(); i++) {
			char currentChar = pattern.charAt(i);
			if (currentChar >= 'a' && currentChar <= 'z' || currentChar >= '0' && currentChar <= '9') {
				tempBuffer.append(currentChar);
			} else {
				if (tempBuffer.length() > 0) {
					String value = tempBuffer.toString();
					if (columnSet.contains(value)) {
						buffer.append("`").append(value).append("`");
					} else {
						buffer.append(value);
					}
					tempBuffer.setLength(0);
				}
				buffer.append(currentChar);
			}
		}
		if (tempBuffer.length() > 0) {
			String value = tempBuffer.toString();
			if (columnSet.contains(value)) {
				buffer.append("`").append(value).append("`");
			} else {
				buffer.append(value);
			}
			tempBuffer.setLength(0);
		}
		return buffer.toString();
	}
}