package com.alibaba.alink.operator.common.stream.model;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.io.directreader.DataBridge;
import com.alibaba.alink.common.io.directreader.DirectReader;
import com.alibaba.alink.common.io.filesystem.AkStream;
import com.alibaba.alink.common.io.filesystem.AkStream.AkWriter.AkCollector;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.io.filesystem.AkUtils.AkMeta;
import com.alibaba.alink.common.io.filesystem.AkUtils.FileForEachReaderIterator;
import com.alibaba.alink.common.io.filesystem.BaseFileSystem;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.params.mapper.ModelMapperParams;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class ModelStreamUtils {

	public static final String MODEL_STREAM_TIMESTAMP_COLUMN_NAME = "alinkmodelstreamtimestamp";
	public static final TypeInformation <?> MODEL_STREAM_TIMESTAMP_COLUMN_TYPE = Types.SQL_TIMESTAMP;
	public static final String MODEL_STREAM_COUNT_COLUMN_NAME = "alinkmodelstreamcount";
	public static final TypeInformation <?> MODEL_STREAM_COUNT_COLUMN_TYPE = Types.LONG;

	private static final Logger LOG = LoggerFactory.getLogger(ModelStreamUtils.class);

	public static boolean useModelStreamFile(Params params) {
		return params != null
			&& params.get(ModelMapperParams.MODEL_STREAM_FILE_PATH) != null;
	}

	public static TableSchema getRawModelSchema(TableSchema modelStreamSchema, int timestampColIndex,
												int countColIndex) {
		int fieldCount = modelStreamSchema.getFieldNames().length;

		String[] rawNames = new String[fieldCount - 2];
		TypeInformation <?>[] rawTypes = new TypeInformation <?>[fieldCount - 2];
		int counter = 0;
		for (int i = 0; i < fieldCount; ++i) {
			if (i == timestampColIndex || i == countColIndex) {
				continue;
			}

			rawNames[counter] = modelStreamSchema.getFieldNames()[i];
			rawTypes[counter++] = modelStreamSchema.getFieldTypes()[i];
		}

		return new TableSchema(rawNames, rawTypes);
	}

	public static class ModelStreamFileSource implements Iterable <Row> {
		private final FilePath filePath;
		private final long scanInterval;
		private final Timestamp startTime;

		private transient ModelStreamFileIdScanner scanner;
		private transient Iterator <Timestamp> fileIdIterator;
		private transient Iterator <Row> modelIterator;

		public ModelStreamFileSource(FilePath filePath, long scanInterval, Timestamp startTime) {
			this.filePath = filePath;
			this.scanInterval = scanInterval;
			this.startTime = startTime;
		}

		public void open() {
			scanner = new ModelStreamFileIdScanner(filePath, startTime, scanInterval);
			scanner.open();
			fileIdIterator = scanner.iterator();
		}

		public void close() throws IOException {
			scanner.close();
		}

		private class ModelStreamSourceIterator implements Iterator <Row> {
			private transient Tuple3 <Timestamp, Long, FilePath> modelDescCache;

			@Override
			public boolean hasNext() {

				if (modelIterator == null || !modelIterator.hasNext()) {
					// switch next fileId

					if (!fileIdIterator.hasNext()) {
						return false;
					}

					while (fileIdIterator.hasNext()) {
						FileForEachReaderIterator fileForEachReaderIterator = new FileForEachReaderIterator();

						modelDescCache = descModel(filePath, fileIdIterator.next());

						try {
							AkUtils.getFromFolderForEach(modelDescCache.f2, fileForEachReaderIterator);
						} catch (IOException e) {
							continue;
						}

						modelIterator = fileForEachReaderIterator.iterator();

						if (modelIterator.hasNext()) {
							return true;
						}
					}

					return false;
				}

				return true;
			}

			@Override
			public Row next() {
				return genRowWithIdentifierInternal(modelIterator.next(), modelDescCache.f0, modelDescCache.f1);
			}
		}

		@Override
		public Iterator <Row> iterator() {
			return new ModelStreamSourceIterator();
		}
	}

	public static TableSchema createSchemaWithModelStreamPrefix(TableSchema tableSchema) {
		return new TableSchema(
			ArrayUtils.addAll(
				new String[] {
					MODEL_STREAM_TIMESTAMP_COLUMN_NAME,
					MODEL_STREAM_COUNT_COLUMN_NAME
				},
				tableSchema.getFieldNames()
			),
			ArrayUtils.addAll(
				new TypeInformation <?>[] {
					MODEL_STREAM_TIMESTAMP_COLUMN_TYPE,
					MODEL_STREAM_COUNT_COLUMN_TYPE
				},
				tableSchema.getFieldTypes()
			)
		);
	}

	public static Row genRowWithIdentifierInternal(Row raw, Timestamp timestamp, Long count) {
		// timestamp is the first column and count is the second column in the internal constructor.
		return genRowWithIdentifier(raw, timestamp, count, 0, 1);
	}

	public static Row genRowWithIdentifier(
		Row raw, Timestamp timestamp, Long count, final int timestampColIndex, final int countColIndex) {

		// append timestamp and count to the raw record.
		int len = raw.getArity() + 2;
		Row rowWithIdentifier = new Row(len);

		int counter = 0;
		for (int i = 0; i < len; ++i) {
			if (i == timestampColIndex) {
				rowWithIdentifier.setField(i, timestamp);
			} else if (i == countColIndex) {
				rowWithIdentifier.setField(i, count);
			} else {
				rowWithIdentifier.setField(i, raw.getField(counter++));
			}
		}

		return rowWithIdentifier;
	}

	public static long getCountFromRowInternal(Row row) {
		return (long) row.getField(1);
	}

	public static Row genRowWithoutIdentifierInternal(Row rowWithId) {
		return genRowWithoutIdentifier(rowWithId, 0, 1);
	}

	public static Row genRowWithoutIdentifier(Row rowWithId, final int timestampColIndex, final int countColIndex) {
		Row ret = new Row(rowWithId.getArity() - 2);

		int counter = 0;

		for (int i = 0; i < rowWithId.getArity(); ++i) {
			if (i == timestampColIndex || i == countColIndex) {
				continue;
			}
			ret.setField(counter++, rowWithId.getField(i));
		}

		return ret;
	}

	public static Tuple2 <TableSchema, List <Row>> readModelRows(FilePath filePath, Timestamp modelId)
		throws IOException {
		Tuple3 <Timestamp, Long, FilePath> modelDesc = ModelStreamUtils.descModel(filePath, modelId);

		return AkUtils.readFromPath(modelDesc.f2);
	}

	public enum ScanTaskStatus {
		CREATED,
		RUNNING,
		CANCELED,
		FAILED
	}

	public static class ScanTask implements Runnable {
		private volatile ScanTaskStatus status;
		private volatile Throwable error;

		private final ExecutorService executorService;

		private transient Future <?> future;

		public ScanTask(ExecutorService executorService) {
			this.executorService = executorService;
		}

		@Override
		public void run() {
			try {

				status = ScanTaskStatus.CREATED;

				doRun();
			} catch (Throwable t) {
				if (t instanceof ScanTaskCancelException) {
					status = ScanTaskStatus.CANCELED;
					error = t;

					return;
				}

				status = ScanTaskStatus.FAILED;
				error = t;
			}
		}

		public void start() {
			future = executorService.submit(this);
		}

		public void cancel() {
			boolean cancelResult = future.cancel(true);
		}

		public void doRun() throws InterruptedException, ScanTaskCancelException {

		}

		private static class ScanTaskCancelException extends RuntimeException {

			public ScanTaskCancelException() {
			}

			public ScanTaskCancelException(String message) {
				super(message);
			}

			public ScanTaskCancelException(String message, Throwable cause) {
				super(message, cause);
			}

			public ScanTaskCancelException(Throwable cause) {
				super(cause);
			}

			public ScanTaskCancelException(String message, Throwable cause, boolean enableSuppression,
										   boolean writableStackTrace) {
				super(message, cause, enableSuppression, writableStackTrace);
			}
		}
	}

	public enum ReadMode {
		CONTINUOUS,
		ONCE
	}

	public static class ModelStreamFileIdScanner implements Iterable <Timestamp> {
		private final static int BLOCK_QUEUE_CAP = 512;

		private final FilePath filePath;
		private final Timestamp startTime;
		private final long scanInterval;

		private transient BlockingQueue <Timestamp> queue;
		private transient Thread monitorThread;
		private transient FileModelStreamSourceMonitor monitor;

		public ModelStreamFileIdScanner(FilePath filePath, Timestamp startTime, long scanInterval) {
			this.filePath = filePath;
			this.startTime = startTime;
			this.scanInterval = scanInterval;
		}

		public void open() {
			queue = new LinkedBlockingQueue <>(BLOCK_QUEUE_CAP);

			monitor = new FileModelStreamSourceMonitor(filePath, new BlockQueueCollector(), scanInterval);

			monitorThread = new Thread(() -> {
				try {
					monitor.startFrom(startTime, ReadMode.CONTINUOUS);
				} catch (InterruptedException | IOException e) {
					throw new IllegalStateException(e);
				}
			});

			monitorThread.start();
		}

		public void close() throws IOException {
			// pass
			monitor.cancel();

			try {
				monitorThread.join();
			} catch (InterruptedException e) {
				throw new IllegalStateException(e);
			}
		}

		@Override
		public Iterator <Timestamp> iterator() {
			return new ModelStreamFileIdIterator();
		}

		private class ModelStreamFileIdIterator implements Iterator <Timestamp> {
			private transient Timestamp cache;

			@Override
			public boolean hasNext() {
				if (cache == null) {
					try {
						cache = queue.take();
					} catch (InterruptedException e) {
						throw new IllegalStateException(e);
					}
				}

				return true;
			}

			@Override
			public Timestamp next() {
				if (cache == null) {
					throw new IllegalStateException("Should call hasNext first.");
				}

				Timestamp ret = cache;
				cache = null;
				return ret;
			}
		}

		private class BlockQueueCollector implements Collector <Timestamp> {

			@Override
			public void collect(Timestamp record) {
				try {
					queue.put(record);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public void close() {
				// pass
			}
		}
	}

	public static TableSchema getSchemaFromFolder(FilePath filePath) throws IOException {
		List <Timestamp> models = listModels(filePath);

		if (models.isEmpty()) {
			throw new IllegalArgumentException("Stream model is empty. path: " + filePath.getPath().toString());
		}

		Timestamp timestamp = models.get(0);

		AkMeta meta = AkUtils.getMetaFromPath(
			new FilePath(new Path(filePath.getPath(), toStringPresentation(timestamp)), filePath.getFileSystem())
		);

		return CsvUtil.schemaStr2Schema(meta.schemaStr);
	}

	public static Tuple3 <Timestamp, Long, FilePath> descModel(FilePath filePath, Timestamp timestamp) {
		BaseFileSystem <?> fileSystem = filePath.getFileSystem();
		String modelId = toStringPresentation(timestamp);

		Path confPath = new Path(
			new Path(filePath.getPath(), FileModelStreamSink.MODEL_CONF),
			String.format("%s.log", modelId)
		);

		ModelStreamMeta meta;
		try (FSDataInputStream fsDataInputStream = filePath.getFileSystem().open(confPath)) {
			meta = JsonConverter.fromJson(IOUtils.toString(fsDataInputStream), ModelStreamMeta.class);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}

		Path modelFolderPath = new Path(filePath.getPath(), modelId);

		try {
			if (!fileSystem.exists(modelFolderPath)) {
				throw new IllegalStateException("Model " + modelFolderPath.getPath() + " is not exists.");
			}
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}

		return Tuple3.of(
			timestamp,
			meta.count,
			new FilePath(modelFolderPath, fileSystem)
		);
	}

	public static List <FilePath> listModelFiles(FilePath modelFolder) throws IOException {
		return modelFolder
			.getFileSystem()
			.listFiles(modelFolder.getPath())
			.stream()
			.map(x -> new FilePath(x, modelFolder.getFileSystem()))
			.collect(Collectors.toList());
	}

	public static class FileModelStreamSourceMonitor {
		private final FilePath filePath;

		private final Collector <Timestamp> collector;
		private final long scanInterval;
		private volatile boolean isRunning = false;

		public FileModelStreamSourceMonitor(FilePath filePath, Collector <Timestamp> collector, long scanInterval) {
			this.filePath = filePath;

			this.collector = collector;
			this.scanInterval = scanInterval;
		}

		public void startFrom(Timestamp startTime, ReadMode readMode) throws IOException, InterruptedException {

			this.isRunning = true;

			switch (readMode) {
				case CONTINUOUS:

					List <Timestamp> latest = null;

					while (isRunning) {

						List <Timestamp> files = filter(listModels(filePath), startTime);
						if (latest == null) {
							latest = files;
						} else {
							files.removeAll(latest);
							latest.addAll(files);
						}

						for (Timestamp file : files) {
							collector.collect(file);
						}

						Thread.sleep(scanInterval);
					}

					return;

				case ONCE:

					if (!isRunning) {
						return;
					}

					for (Timestamp file : filter(listModels(filePath), startTime)) {
						collector.collect(file);
					}

					return;
				default:
					throw new UnsupportedOperationException();
			}
		}

		public void cancel() {
			this.isRunning = false;
		}
	}

	public static class ModelStreamMeta {
		public long count;
		public int numFiles;

		public ModelStreamMeta() {
		}

		public ModelStreamMeta(long count, int numFiles) {
			this.count = count;
			this.numFiles = numFiles;
		}
	}

	/**
	 * Structure of model stream
	 * <p>root
	 * - conf - id0 - id1
	 */
	public static class FileModelStreamSink implements Serializable {
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

			BaseFileSystem <?> fileSystem = filePath.getFileSystem();

			// construct model folder
			Path confDirPath = new Path(filePath.getPath(), MODEL_CONF);

			Path modelPath = new Path(confDirPath, ModelStreamUtils.toStringPresentation(modelId));

			if (fileSystem.exists(modelPath)) {
				throw new IOException(String.format("ModelPath: %s has existed.", modelPath));
			} else {
				fileSystem.mkdirs(modelPath);
			}

			for (int i = 0; i < numFiles; ++i) {
				Path subInProgressModelFilePath = new Path(confDirPath,
					String.format("%s_%d", ModelStreamUtils.toStringPresentation(modelId), i));
				Path subToCommitModelFilePath = new Path(modelPath, String.valueOf(i));

				fileSystem.rename(subInProgressModelFilePath, subToCommitModelFilePath);
			}

			// if done, write redo log.
			Path logPath = new Path(confDirPath,
				String.format("%s.log", ModelStreamUtils.toStringPresentation(modelId)));

			try (FSDataOutputStream outputStream = fileSystem.create(logPath, WriteMode.OVERWRITE)) {
				outputStream.write(JsonConverter.toJson(new ModelStreamMeta(numRows, numFiles)).getBytes());
			} catch (Exception ex) {
				// if write fail, delete the redo log to make the model invalid.
				fileSystem.delete(logPath, false);
				throw ex;
			}

			// if done, do commit.
			Path finalModelPath = new Path(filePath.getPath(), ModelStreamUtils.toStringPresentation(modelId));
			fileSystem.rename(modelPath, finalModelPath);

			// if done, do clean up
			cleanUp(filePath, numKeepModel);
		}
	}

	public static Timestamp createStartTime(String startTimeStr) {
		Timestamp startTime = new Timestamp(System.currentTimeMillis());

		if (!StringUtils.isNullOrWhitespaceOnly(startTimeStr)) {
			startTime = Timestamp.valueOf(startTimeStr);
		}

		return startTime;
	}

	public static long createScanIntervalMillis(int scanIntervalSecond) {
		return 1000L * scanIntervalSecond;
	}

	public static TableSchema createSchemaFromFilePath(FilePath filePath, String schemaStr) {
		TableSchema schema;

		if (schemaStr != null) {
			schema = CsvUtil.schemaStr2Schema(schemaStr);
		} else {
			try {
				schema = ModelStreamUtils.getSchemaFromFolder(filePath);
			} catch (Exception e) {
				throw new IllegalArgumentException("Should set the schema str when the model folder is empty.", e);
			}
		}

		return schema;
	}

	public static DataStream <Row> broadcastStream(DataStream <Row> input) {
		return input
			.flatMap(new RichFlatMapFunction <Row, Tuple2 <Integer, Row>>() {
				private static final long serialVersionUID = 6421400378693673120L;

				@Override
				public void flatMap(Row row, Collector <Tuple2 <Integer, Row>> out)
					throws Exception {
					int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
					for (int i = 0; i < numTasks; ++i) {
						out.collect(Tuple2.of(i, row));
					}
				}
			}).partitionCustom(new Partitioner <Integer>() {

				@Override
				public int partition(Integer key, int numPartitions) { return key; }
			}, 0).map(new MapFunction <Tuple2 <Integer, Row>, Row>() {

				@Override
				public Row map(Tuple2 <Integer, Row> value) throws Exception {
					return value.f1;
				}
			});
	}

	public static class PredictProcess extends RichCoFlatMapFunction <Row, Row, Row> {

		private final DataBridge dataBridge;
		private ModelMapper mapper;
		private final Map <Timestamp, List <Row>> buffers = new HashMap <>();
		private final int timestampColIndex;
		private final int countColIndex;

		public PredictProcess(
			TableSchema modelSchema, TableSchema dataSchema, Params params,
			TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder,
			DataBridge dataBridge, int timestampColIndex, int countColIndex) {

			this.dataBridge = dataBridge;
			this.mapper = mapperBuilder.apply(modelSchema, dataSchema, params);
			this.timestampColIndex = timestampColIndex;
			this.countColIndex = countColIndex;
		}

		@Override
		public void open(Configuration parameters) throws Exception {

			if (dataBridge != null) {
				// read init model
				List <Row> modelRows = DirectReader.directRead(dataBridge);
				this.mapper.loadModel(modelRows);
				this.mapper.open();
			}
		}

		@Override
		public void close() throws Exception {
			super.close();
			this.mapper.close();
		}

		@Override
		public void flatMap1(Row row, Collector <Row> collector) throws Exception {
			collector.collect(this.mapper.map(row));
		}

		@Override
		public void flatMap2(Row inRow, Collector <Row> collector) throws Exception {
			Timestamp timestamp = (Timestamp) inRow.getField(timestampColIndex);
			long count = (long) inRow.getField(countColIndex);

			Row row = genRowWithoutIdentifier(inRow, timestampColIndex, countColIndex);

			if (buffers.containsKey(timestamp) && buffers.get(timestamp).size() == (int) count - 1) {
				if (buffers.containsKey(timestamp)) {
					buffers.get(timestamp).add(row);
				} else {
					List <Row> buffer = new ArrayList <>(0);
					buffer.add(row);
					buffers.put(timestamp, buffer);
				}

				ModelMapper modelMapper = this.mapper.createNew(buffers.get(timestamp));
				modelMapper.open();

				this.mapper = modelMapper;

				buffers.get(timestamp).clear();
			} else {
				if (buffers.containsKey(timestamp)) {
					buffers.get(timestamp).add(row);
				} else {
					List <Row> buffer = new ArrayList <>(0);
					buffer.add(row);
					buffers.put(timestamp, buffer);
				}
			}
		}
	}

	private static final int YEAR_LENGTH = 4;
	private static final int MONTH_LENGTH = 2;
	private static final int DAY_LENGTH = 2;
	private static final int MAX_MONTH = 12;
	private static final int MAX_DAY = 31;
	private static final int TIME_LENGTH = 2;

	private static Timestamp fromStringInternal(String s) {
		String dateS;
		String timeS;
		String nanosS;
		int year;
		int month;
		int day;
		int hour;
		int minute;
		int second;
		int aNanos;
		String formatError = "Timestamp format must be yyyymmddhhmmss[fffffffff]";
		String zeros = "000000000";

		if (s == null) {
			throw new IllegalArgumentException("null string");
		}

		// Split the string into date and time components
		s = s.trim();

		int dataLen = YEAR_LENGTH + MONTH_LENGTH + DAY_LENGTH;

		if (s.length() < dataLen) {
			throw new IllegalArgumentException(formatError);
		}

		dateS = s.substring(0, YEAR_LENGTH + MONTH_LENGTH + DAY_LENGTH);
		timeS = s.substring(YEAR_LENGTH + MONTH_LENGTH + DAY_LENGTH);

		// Parse the time
		if (timeS.isEmpty()) {
			throw new IllegalArgumentException(formatError);
		}

		// Convert the date
		String yyyy = dateS.substring(0, YEAR_LENGTH);
		String mm = dateS.substring(YEAR_LENGTH, YEAR_LENGTH + MONTH_LENGTH);
		String dd = dateS.substring(YEAR_LENGTH + MONTH_LENGTH);
		year = Integer.parseInt(yyyy);
		month = Integer.parseInt(mm);
		day = Integer.parseInt(dd);

		if (!((month >= 1 && month <= MAX_MONTH) && (day >= 1 && day <= MAX_DAY))) {
			throw new IllegalArgumentException(formatError);
		}

		int timeLen = TIME_LENGTH * 3;
		// Convert the time; default missing nanos
		if (timeS.length() >= timeLen) {
			hour = Integer.parseInt(timeS.substring(0, TIME_LENGTH));
			minute =
				Integer.parseInt(timeS.substring(TIME_LENGTH, TIME_LENGTH * 2));
			second =
				Integer.parseInt(timeS.substring(TIME_LENGTH * 2, timeLen));

			if (timeS.length() > timeLen) {
				nanosS = timeS.substring(timeLen);
				if (nanosS.length() > 9) {
					throw new IllegalArgumentException(formatError);
				}
				if (!Character.isDigit(nanosS.charAt(0))) {
					throw new IllegalArgumentException(formatError);
				}
				nanosS = nanosS + zeros.substring(0,9-nanosS.length());
				aNanos = Integer.parseInt(nanosS);
			} else {
				throw new IllegalArgumentException(formatError);
			}
		} else {
			throw new IllegalArgumentException(formatError);
		}

		return new Timestamp(year - 1900, month - 1, day, hour, minute, second, aNanos);
	}

	private static String toStringInternal (Timestamp timestamp) {
		int year = timestamp.getYear() + 1900;
		int month = timestamp.getMonth() + 1;
		int day = timestamp.getDate();
		int hour = timestamp.getHours();
		int minute = timestamp.getMinutes();
		int second = timestamp.getSeconds();
		int nanos = timestamp.getNanos();

		String yearString;
		String monthString;
		String dayString;
		String hourString;
		String minuteString;
		String secondString;
		String nanosString;
		String zeros = "000000000";
		String yearZeros = "0000";
		StringBuffer timestampBuf;

		if (year < 1000) {
			// Add leading zeros
			yearString = "" + year;
			yearString = yearZeros.substring(0, (4-yearString.length())) +
				yearString;
		} else {
			yearString = "" + year;
		}
		if (month < 10) {
			monthString = "0" + month;
		} else {
			monthString = Integer.toString(month);
		}
		if (day < 10) {
			dayString = "0" + day;
		} else {
			dayString = Integer.toString(day);
		}
		if (hour < 10) {
			hourString = "0" + hour;
		} else {
			hourString = Integer.toString(hour);
		}
		if (minute < 10) {
			minuteString = "0" + minute;
		} else {
			minuteString = Integer.toString(minute);
		}
		if (second < 10) {
			secondString = "0" + second;
		} else {
			secondString = Integer.toString(second);
		}
		if (nanos == 0) {
			nanosString = "0";
		} else {
			nanosString = Integer.toString(nanos);

			// Add leading zeros
			nanosString = zeros.substring(0, (9-nanosString.length())) +
				nanosString;

			// Truncate trailing zeros
			char[] nanosChar = new char[nanosString.length()];
			nanosString.getChars(0, nanosString.length(), nanosChar, 0);
			int truncIndex = 8;
			while (nanosChar[truncIndex] == '0') {
				truncIndex--;
			}

			nanosString = new String(nanosChar, 0, truncIndex + 1);
		}

		// do a string buffer here instead.
		timestampBuf = new StringBuffer(20+nanosString.length());
		timestampBuf.append(yearString);
		timestampBuf.append(monthString);
		timestampBuf.append(dayString);
		timestampBuf.append(hourString);
		timestampBuf.append(minuteString);
		timestampBuf.append(secondString);
		timestampBuf.append(nanosString);

		return (timestampBuf.toString());
	}

	public static Timestamp fromString(String string) {
		return fromStringInternal(string);
	}

	public static String toStringPresentation(Timestamp timestamp) {
		return toStringInternal(timestamp);
	}

	private static List <Timestamp> listModels(FilePath filePath) throws IOException {
		FileStatus[] fileStatuses = filePath.getFileSystem().listStatus(filePath.getPath());

		List <Timestamp> allModels = new ArrayList <>();

		for (FileStatus fileStatus : fileStatuses) {
			if (fileStatus.isDir()) {
				String folderName = fileStatus.getPath().getName();
				Timestamp timestamp;

				try {
					timestamp = fromString(folderName);
				} catch (Exception ex) {
					// pass

					continue;
				}

				allModels.add(timestamp);
			}
		}

		return allModels;
	}

	private static void cleanUp(FilePath filePath, int numKeepModel) throws IOException {

		if (numKeepModel < 0) {
			return;
		}

		List <Timestamp> models = listModels(filePath);

		models.sort(Timestamp::compareTo);

		BaseFileSystem <?> fileSystem = filePath.getFileSystem();
		Path confFolder = new Path(filePath.getPath(), FileModelStreamSink.MODEL_CONF);

		for (int i = 0; i < models.size() - numKeepModel; ++i) {
			// do remove

			// remove model
			fileSystem.delete(new Path(filePath.getPath(), toStringPresentation(models.get(i))), true);

			// remove log
			fileSystem.delete(new Path(confFolder, String.format("%s.log", toStringPresentation(models.get(i)))),
				false);
		}
	}

	private static List <Timestamp> filter(List <Timestamp> input, final Timestamp startTimestamp) {
		return input.stream()
			.filter(timestamp -> timestamp.compareTo(startTimestamp) >= 0)
			.collect(Collectors.toList());
	}

	public static int findTimestampColIndexWithAssertAndHint(TableSchema schema) {
		return findColIndexWithAssertAndHint(
			schema,
			MODEL_STREAM_TIMESTAMP_COLUMN_NAME,
			MODEL_STREAM_TIMESTAMP_COLUMN_TYPE,
			"The type of model stream timestamp column should be %s, but %s is given.");
	}

	public static int findCountColIndexWithAssertAndHint(TableSchema schema) {
		return findColIndexWithAssertAndHint(schema,
			MODEL_STREAM_COUNT_COLUMN_NAME,
			MODEL_STREAM_COUNT_COLUMN_TYPE,
			"The type of model stream count column should be %s, but %s was given.");
	}

	public static int findColIndexWithAssertAndHint(
		TableSchema schema, String colName,
		TypeInformation <?> colType, String s) {

		final int colIndex = TableUtil.findColIndexWithAssertAndHint(
			schema, colName
		);

		Preconditions.checkState(
			schema.getFieldTypes()[colIndex].equals(colType),
			String.format(s,
				FlinkTypeConverter.getTypeString(colType),
				FlinkTypeConverter.getTypeString(schema.getFieldTypes()[colIndex])
			)
		);

		return colIndex;
	}
}
