package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.OverTimeWindowStreamOp;
import com.alibaba.alink.operator.stream.feature.TumbleTimeWindowStreamOp;
import com.alibaba.alink.params.io.Export2FileSinkParams;
import org.apache.commons.lang.math.NumberUtils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

		final String partitionFormat = getPartitionsFormat();

		String windowTime = getWindowTime();

		List <Tuple2 <String, SimpleDateFormat>> dateFormats = null;

		if (partitionFormat != null) {
			dateFormats = new ArrayList <>();
			String[] folders = partitionFormat.split("/");
			String maxTimeUnit = null;

			for (String folder : folders) {
				String[] kv = folder.split("=");
				AkPreconditions.checkState(kv.length == 2, "The length of key-value should be 2.");

				String key = kv[0];
				String value = kv[1];

				dateFormats.add(Tuple2.of(key, new SimpleDateFormat(value)));

				for (int j = 0; j < value.length(); ++j) {
					maxTimeUnit = max(maxTimeUnit, value.substring(j, j + 1));
				}
			}

			if (maxTimeUnit == null) {
				throw new IllegalArgumentException("There is no time format str in data format.");
			}

			TimeUnitEnum timeUnitEnum = CHAR_2_TIME_UNIT.get(maxTimeUnit);
			TimeUnitEnum windowTimeUnitEnum = windowTime2TimeUnit(windowTime);

			if (timeUnitEnum.getID() > windowTimeUnitEnum.getID()) {
				throw new IllegalArgumentException(
					String.format(
						"Window time is greater than partitions format. window time: %s, partitions format: %s",
						windowTime, partitionFormat
					)
				);
			}
		}

		StringBuilder cols = new StringBuilder(names[0]);
		for (int i = 1; i < names.length; i++) {
			cols.append(",").append(names[i]);
		}

		final String windowStartCol = "window_start";
		final String mTableCol = "mt";

		StreamOperator <?> stream;

		if (timeCol == null) {

			timeCol = "ts";

			stream = in
				.select(String.format("LOCALTIMESTAMP AS %s, %s", timeCol, cols))
				.link(
					new TumbleTimeWindowStreamOp()
						.setTimeCol(timeCol)
						.setWindowTime(windowTime)
						.setClause(
							String.format("TUMBLE_START() as %s, MTABLE_AGG( %s ) AS %s", windowStartCol, cols, mTableCol)
						)
				);
		} else {
			stream = in
				.link(
					new TumbleTimeWindowStreamOp()
						.setTimeCol(timeCol)
						.setWindowTime(windowTime)
						.setClause(
							String.format("TUMBLE_START() as %s, MTABLE_AGG( %s ) AS %s", windowStartCol, cols, mTableCol)
						)
				);
		}

		final int windowStartColIndex = TableUtil.findColIndexWithAssert(stream.getSchema(), windowStartCol);
		final int mTableColIndex = TableUtil.findColIndexWithAssert(stream.getSchema(), mTableCol);

		stream
			.getDataStream()
			.addSink(new OutputFormatSinkFunction <>(
				new Export2FileOutputFormat(
					getFilePath(),
					getOverwriteSink() ? FileSystem.WriteMode.OVERWRITE : FileSystem.WriteMode.NO_OVERWRITE,
					dateFormats,
					windowStartColIndex,
					mTableColIndex
				)
			))
			.name("export-2-file-sink");

		return this;
	}

	private final static Map <String, TimeUnitEnum> CHAR_2_TIME_UNIT;

	private enum TimeUnitEnum {
		OTHER(0),
		YEAR(1),
		MONTH(2),
		WEEK(3),
		DAY(4),
		HOUR(5),
		MINUTE(6),
		SECOND(7),
		MILLI_SECOND(8);

		private final int id;

		TimeUnitEnum(int id) {
			this.id = id;
		}

		public int getID() {
			return id;
		}
	}

	static {
		HashMap <String, TimeUnitEnum> m = new HashMap <>();

		m.put("G", TimeUnitEnum.YEAR);
		m.put("y", TimeUnitEnum.YEAR);
		m.put("Y", TimeUnitEnum.YEAR);
		m.put("M", TimeUnitEnum.MONTH);
		m.put("L", TimeUnitEnum.MONTH);
		m.put("w", TimeUnitEnum.WEEK);
		m.put("W", TimeUnitEnum.WEEK);
		m.put("D", TimeUnitEnum.DAY);
		m.put("d", TimeUnitEnum.DAY);
		m.put("F", TimeUnitEnum.DAY);
		m.put("u", TimeUnitEnum.DAY);
		m.put("a", TimeUnitEnum.HOUR);
		m.put("H", TimeUnitEnum.HOUR);
		m.put("k", TimeUnitEnum.HOUR);
		m.put("K", TimeUnitEnum.HOUR);
		m.put("h", TimeUnitEnum.HOUR);
		m.put("m", TimeUnitEnum.MINUTE);
		m.put("s", TimeUnitEnum.SECOND);
		m.put("S", TimeUnitEnum.MILLI_SECOND);
		m.put("z", TimeUnitEnum.OTHER);
		m.put("Z", TimeUnitEnum.OTHER);
		m.put("X", TimeUnitEnum.OTHER);

		CHAR_2_TIME_UNIT = Collections.unmodifiableMap(m);
	}

	private static String max(String a, String b) {
		TimeUnitEnum aVal = CHAR_2_TIME_UNIT.get(a);
		TimeUnitEnum bVal = CHAR_2_TIME_UNIT.get(b);

		if (aVal == null && bVal == null) {
			return null;
		}

		if (aVal == null) {
			return b;
		}

		if (bVal == null) {
			return a;
		}

		return aVal.getID() < bVal.getID() ? b : a;
	}

	private static TimeUnitEnum windowTime2TimeUnit(String windowTime) {
		if (NumberUtils.isNumber(windowTime)) {
			return windowTime2TimeUnitSecond(Double.parseDouble(windowTime));
		} else {
			windowTime = windowTime.trim();
			String unit = windowTime.substring(windowTime.length() - 1);
			int ti = Integer.parseInt(windowTime.substring(0, windowTime.length() - 1));
			switch (unit) {
				case "s":
				case "m":
				case "h":
				case "d":
					return windowTime2TimeUnitSecond(
						OverTimeWindowStreamOp.getIntervalBySecond(windowTime)
					);
				case "M":
					if (ti == 1) {
						return TimeUnitEnum.MONTH;
					}
					return TimeUnitEnum.YEAR;
				case "y":
					if (ti == 1) {
						return TimeUnitEnum.YEAR;
					}
				default:
					throw new AkIllegalOperatorParameterException("Is is not support time format. " + windowTime);
			}
		}
	}

	private static TimeUnitEnum windowTime2TimeUnitSecond(double windowTime) {
		if (windowTime == 0.001) {
			return TimeUnitEnum.MILLI_SECOND;
		} else if (windowTime <= 1) {
			return TimeUnitEnum.SECOND;
		} else if (windowTime <= 60) {
			return TimeUnitEnum.MINUTE;
		} else if (windowTime <= 3600) {
			return TimeUnitEnum.HOUR;
		} else if (windowTime <= 3600 * 24) {
			return TimeUnitEnum.DAY;
		} else if (windowTime <= 3600 * 24 * 7) {
			return TimeUnitEnum.WEEK;
		} else {
			throw new IllegalArgumentException("Window time should be <= 604800s and  when it is an double");
		}
	}
}
