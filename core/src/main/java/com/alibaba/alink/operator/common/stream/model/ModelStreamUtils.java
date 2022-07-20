package com.alibaba.alink.operator.common.stream.model;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.exceptions.AkIllegalModelException;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.io.filesystem.AkUtils.AkMeta;
import com.alibaba.alink.common.io.filesystem.BaseFileSystem;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.params.ModelStreamScanParams;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ModelStreamUtils {

	private static final Logger LOG = LoggerFactory.getLogger(ModelStreamUtils.class);

	public static final String MODEL_STREAM_TIMESTAMP_COLUMN_NAME = "alinkmodelstreamtimestamp";
	public static final TypeInformation <?> MODEL_STREAM_TIMESTAMP_COLUMN_TYPE = Types.SQL_TIMESTAMP;
	public static final String MODEL_STREAM_COUNT_COLUMN_NAME = "alinkmodelstreamcount";
	public static final TypeInformation <?> MODEL_STREAM_COUNT_COLUMN_TYPE = Types.LONG;

	public static boolean useModelStreamFile(Params params) {
		return params != null
			&& params.get(ModelStreamScanParams.MODEL_STREAM_FILE_PATH) != null;
	}

	public static TableSchema getRawModelSchema(
		TableSchema modelStreamSchema, int timestampColIndex, int countColIndex) {

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
		throws Exception {
		Tuple3 <Timestamp, Long, FilePath> modelDesc = ModelStreamUtils.descModel(filePath, modelId);

		return AkUtils.readFromPath(modelDesc.f2);
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

		return TableUtil.schemaStr2Schema(meta.schemaStr);
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

	public static List <Timestamp> listModels(FilePath filePath) throws IOException {
		FileStatus[] fileStatuses = filePath.getFileSystem().listStatus(filePath.getPath());

		List <Timestamp> allModels = new ArrayList <>();

		for (FileStatus fileStatus : fileStatuses) {
			if (fileStatus.isDir()) {
				String folderName = fileStatus.getPath().getName();
				Timestamp timestamp;

				try {
					timestamp = ModelStreamUtils.fromString(folderName);
				} catch (Exception ex) {
					// pass

					continue;
				}

				allModels.add(timestamp);
			}
		}

		return allModels;
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
			schema = TableUtil.schemaStr2Schema(schemaStr);
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
				public int partition(Integer key, int numPartitions) {return key;}
			}, 0).map(new MapFunction <Tuple2 <Integer, Row>, Row>() {

				@Override
				public Row map(Tuple2 <Integer, Row> value) throws Exception {
					return value.f1;
				}
			});
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
			throw new java.lang.IllegalArgumentException("null string");
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
			throw new java.lang.IllegalArgumentException(formatError);
		}

		// Convert the date
		String yyyy = dateS.substring(0, YEAR_LENGTH);
		String mm = dateS.substring(YEAR_LENGTH, YEAR_LENGTH + MONTH_LENGTH);
		String dd = dateS.substring(YEAR_LENGTH + MONTH_LENGTH);
		year = Integer.parseInt(yyyy);
		month = Integer.parseInt(mm);
		day = Integer.parseInt(dd);

		if (!((month >= 1 && month <= MAX_MONTH) && (day >= 1 && day <= MAX_DAY))) {
			throw new java.lang.IllegalArgumentException(formatError);
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
					throw new java.lang.IllegalArgumentException(formatError);
				}
				if (!Character.isDigit(nanosS.charAt(0))) {
					throw new java.lang.IllegalArgumentException(formatError);
				}
				nanosS = nanosS + zeros.substring(0, 9 - nanosS.length());
				aNanos = Integer.parseInt(nanosS);
			} else {
				throw new java.lang.IllegalArgumentException(formatError);
			}
		} else {
			throw new java.lang.IllegalArgumentException(formatError);
		}

		return new Timestamp(year - 1900, month - 1, day, hour, minute, second, aNanos);
	}

	private static String toStringInternal(Timestamp timestamp) {
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
			yearString = yearZeros.substring(0, (4 - yearString.length())) +
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
			nanosString = zeros.substring(0, (9 - nanosString.length())) +
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
		timestampBuf = new StringBuffer(20 + nanosString.length());
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

	public static FilePath getLatestModelPath(String filePath) throws IOException {
		return getLatestModelPath(new FilePath(filePath));
	}

	public static FilePath getLatestModelPath(FilePath filePath) throws IOException {
		List <Timestamp> timestamps = ModelStreamUtils.listModels(filePath);
		if (timestamps.size() == 0) {
			return null;
		}
		timestamps.sort(Timestamp::compareTo);
		return new FilePath(new Path(filePath.getPath(), toStringPresentation(timestamps.get(timestamps.size() - 1))));
	}

	public static FilePath getEarliestModelPath(String filePath) throws IOException {
		return getEarliestModelPath(new FilePath(filePath));
	}

	public static FilePath getEarliestModelPath(FilePath filePath) throws IOException {
		List <Timestamp> timestamps = ModelStreamUtils.listModels(filePath);
		if (timestamps.size() == 0) {
			return null;
		}
		timestamps.sort(Timestamp::compareTo);
		return new FilePath(new Path(filePath.getPath(), toStringPresentation(timestamps.get(0))));
	}
}
