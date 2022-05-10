package com.alibaba.alink.common.io.filesystem;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.io.filesystem.AkStream.AkReader;
import com.alibaba.alink.common.io.filesystem.AkStream.AkReader.AkReadIterator;
import com.alibaba.alink.common.io.filesystem.copy.FileInputFormat;
import com.alibaba.alink.common.io.filesystem.copy.FileOutputFormat;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.WhereBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.Export2FileSinkStreamOp.Export2FileOutputFormat;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.operator.stream.sql.WhereStreamOp;
import org.apache.commons.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
		FilePath filePath, FilterFunction<Row> filterFunction) throws Exception {

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
				throw new IllegalArgumentException("Could not find the file: " + filePath.getPathStr());
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

				Preconditions.checkState(
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

						throw new RuntimeException(e);
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

	public static Tuple3 <List<Row>, String, String> listPartitions(FilePath filePath, String pattern) throws IOException {
		List<String> columns = getPartitionColumns(filePath);
		String transPattern = transformPattern(pattern, columns);
		int maxDepth = 0;
		for (int i = 0; i < columns.size(); i++) {
			if (pattern.contains(columns.get(i))) {
				maxDepth = i + 1;
			}
		}
		if (maxDepth == 0) {
			throw new RuntimeException(String.format("cannot find partition column in pattern %s", pattern));
		}
		BaseFileSystem <?> baseFileSystem = filePath.getFileSystem();
		Path rootPath = filePath.getPath();
		List<String> pathDirectories = new ArrayList <>();
		getRecursionDirectories(baseFileSystem, rootPath, pathDirectories, maxDepth, 0);
		if (pathDirectories.size() == 0) {
			throw new RuntimeException(String.format("no data in path %s", rootPath.getPath()));
		}
		List<Row> rows = new ArrayList <>(pathDirectories.size());
		for (String pathDirectory : pathDirectories) {
			String[] dirPaths = pathDirectory.split(Path.SEPARATOR);
			Row row = new Row(maxDepth);
			for (int j = 0; j < maxDepth; j++) {
				String[] columnValues = splitPath(dirPaths[dirPaths.length - j - 1]);
				row.setField(maxDepth - j - 1, columnValues[1]);
			}
			rows.add(row);
		}
		StringBuilder buffer = new StringBuilder();
		for (int i = 0; i < maxDepth; i++) {
			buffer.append(columns.get(i));
			buffer.append(" string");
			if (i != maxDepth -1) {
				buffer.append(",");
			}
		}
		String schema = buffer.toString();

		return Tuple3.of(rows, schema, transPattern);
	}

	public static BatchOperator<?> selectPartitionBatchOp(Long mlEnvId, FilePath filePath, String pattern) throws IOException {
		Tuple3<List<Row>, String, String> partitions = listPartitions(filePath, pattern);
		BatchOperator<?> op = new MemSourceBatchOp(partitions.f0, partitions.f1)
			.setMLEnvironmentId(mlEnvId);
		return new WhereBatchOp()
			.setClause(partitions.f2)
			.setMLEnvironmentId(mlEnvId)
			.linkFrom(op);
	}

	public static StreamOperator<?> selectPartitionStreamOp(
		Long mlEnvId, FilePath filePath, String pattern) throws IOException {

		Tuple3<List<Row>, String, String> partitions = listPartitions(filePath, pattern);
		System.out.println(JsonConverter.toJson(partitions));
		StreamOperator <?> op = new MemSourceStreamOp(partitions.f0, partitions.f1);
		return new WhereStreamOp()
			.setClause(partitions.f2)
			.setMLEnvironmentId(mlEnvId)
			.linkFrom(op);
	}

	public static void getRecursionDirectories(BaseFileSystem <?> baseFileSystem,
											   Path p,
											   List<String> pathNames,
											   int maxDepth, int currentDepth) throws IOException {
		List<Path> paths = baseFileSystem.listDirectories(p);
		if (currentDepth + 1 == maxDepth) {
			for (Path path : paths) {
				pathNames.add(path.getPath());
			}
			return;
		}
		for (Path path : paths) {
			getRecursionDirectories(baseFileSystem, path, pathNames, maxDepth, currentDepth + 1);
		}
	}

	public static List<String> getPartitionColumns(FilePath filePath) throws IOException {
		BaseFileSystem <?> baseFileSystem = filePath.getFileSystem();
		List<String> columns = new ArrayList <>();
		Path currentPath = filePath.getPath();
		while (true) {
			List<Path> subdirs = baseFileSystem.listDirectories(currentPath);
			if (subdirs.size() == 0) {
				break;
			}
			currentPath = subdirs.get(0);
			String[] splitValues = splitPath(currentPath.getName());
			columns.add(splitValues[0]);
		}
		return columns;
	}

	public static String[] splitPath(String dirname) {
		String[] splits = dirname.split(COLUMN_SPLIT_TAG);
		if (splits.length != 2) {
			throw new RuntimeException(String.format("invalid directory name %s", dirname));
		}
		return splits;
	}

	public static String transformPattern(String pattern, List<String> columns) {
		String lower = pattern.toLowerCase();
		HashSet<String> columnSet = new HashSet<>(columns);
		StringBuilder buffer = new StringBuilder();
		int start = 0, end = -1;
		for (int i = 0; i < lower.length(); i++) {
			if (lower.charAt(i) < 'a' || lower.charAt(i) > 'z') {
				if (end > start) {
					String column = lower.substring(start, end + 1);
					if (columnSet.contains(column)) {
						buffer.append('`');
						buffer.append(column);
						buffer.append('`');
					} else {
						buffer.append(column);
					}
				}
				buffer.append(lower.charAt(i));
				start = i + 1;
			} else {
				end = i;
			}
		}
		if (end > start) {
			String column = lower.substring(start, end + 1);
			if (columnSet.contains(column)) {
				buffer.append('`');
				buffer.append(column);
				buffer.append('`');
			} else {
				buffer.append(column);
			}
		}
		return buffer.toString();
	}

}
