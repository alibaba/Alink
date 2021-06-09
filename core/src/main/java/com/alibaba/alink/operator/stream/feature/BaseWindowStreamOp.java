package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.feature.featurebuilder.FeatureClauseUtil.ClauseInfo;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.feature.featuregenerator.BaseWindowParams;


import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

import static com.alibaba.alink.operator.common.feature.featurebuilder.WindowResColType.RES_TYPE;

/**
 * Base class for stream feature builder.
 */
abstract class BaseWindowStreamOp <T extends BaseWindowStreamOp <T>>
	extends StreamOperator <T> implements BaseWindowParams <T> {

	String[] inputColNames;

	public BaseWindowStreamOp(Params params) {
		super(params);
	}

	@Override
	public T linkFrom(StreamOperator <?>... inputs) {
		generateWindowClause();
		String registerName = StreamOperator.createUniqueTableName();
		StreamOperator input = checkAndGetFirst(inputs);
		inputColNames = input.getColNames();
		String timeCol = getParams().get(BaseWindowParams.TIME_COL);
		long envId = input.getMLEnvironmentId();
		StreamTableEnvironment env = MLEnvironmentFactory.get(envId).getStreamTableEnvironment();
		StreamExecutionEnvironment exeEnv = MLEnvironmentFactory.get(envId).getStreamExecutionEnvironment();
		exeEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		long offset = Math.round(getLatency() * 1000);
		final int timeIndex = TableUtil.findColIndex(inputColNames, timeCol);

		//change timeCol to watermark
		String[] newColNames = input.getColNames().clone();
		newColNames[timeIndex] = timeCol + ".rowtime";
		TypeInformation[] newColTypes = input.getColTypes().clone();
		newColTypes[timeIndex] = TypeInformation.of(Long.class);
		SingleOutputStreamOperator <Row> rows = input.getDataStream()
			.map(new MapFunction <Row, Row>() {
				@Override
				public Row map(Row value) throws Exception {
					long timestamp = ((Timestamp) value.getField(timeIndex)).getTime();
					timestamp += 28800000;
					value.setField(timeIndex, new Timestamp(timestamp));
					return value;
				}
			});
		DataStream <Row> rowsWithWatermark;
		BaseWindowParams.WatermarkType watermarkType = getWatermarkType();
		if (WatermarkType.PERIOD.equals(watermarkType)) {
			rowsWithWatermark =
				rows.assignTimestampsAndWatermarks(new PeriodExtendBound(Time.of(offset, TimeUnit.MILLISECONDS), timeIndex));
		} else {
			rowsWithWatermark =
				rows.assignTimestampsAndWatermarks(new PunctuatedAssigner (Time.of(offset, TimeUnit.MILLISECONDS), timeIndex));
		}
		rowsWithWatermark = rowsWithWatermark
			.map(new MapFunction <Row, Row>() {
				@Override
				public Row map(Row value) throws Exception {
					Row out = Row.copy(value);
					out.setField(timeIndex, ((Timestamp) out.getField(timeIndex)).getTime());
					return out;
				}
			})
			.name("waterMarkMap")
			.returns(new RowTypeInfo(newColTypes, newColNames));
		Table table = env.fromDataStream(rowsWithWatermark, concatColNames(newColNames));
		env.registerTable(registerName, table);

		Tuple2<ClauseInfo, String> sqlInfo = generateSqlInfo(registerName);
		String sqlClause = sqlInfo.f1;
		if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
			System.out.println(sqlClause);
		}
		StreamOperator res = MLEnvironmentFactory.get(envId).streamSQL(sqlClause).setMLEnvironmentId(getMLEnvironmentId());
		//modify agg result type.
		DataStream<Row> resDataSet = res.getDataStream();
		String[] resNames = res.getColNames();
		TypeInformation[] resTypes = res.getColTypes();
		modifyResType(input.getSchema(), resNames, resTypes, sqlInfo.f0, buildTimeCols(sqlInfo.f0, timeCol));
		this.setOutputTable(DataStreamConversionUtil.toTable(envId, resDataSet, resNames, resTypes));
		return (T) this;
	}

	abstract void generateWindowClause();

	abstract Tuple2 <ClauseInfo, String> generateSqlInfo(String registerName);

	abstract String[] buildTimeCols(ClauseInfo clauseInfo, String timeCol);

	private static void modifyResType(TableSchema inputSchema, String[] resNames,
									 TypeInformation[] resTypes, ClauseInfo clauseInfo, String[] timeCols) {
		for (int i = 0; i < clauseInfo.operatorIndex; i++) {

			String resCol = clauseInfo.asCols[i];
			int resIndex = TableUtil.findColIndex(resNames, resCol);
			if (clauseInfo.operators[i] != null) {
				TypeInformation resType = clauseInfo.operators[i].getResType();
				if (RES_TYPE.equals(resType)) {
					TypeInformation inputColType = TableUtil.findColType(inputSchema, clauseInfo.inputCols[i]);
					resTypes[resIndex] = inputColType;
				} else {
					resTypes[resIndex] = resType;
				}
			} else {
				TypeInformation inputColType = TableUtil.findColType(inputSchema, clauseInfo.inputCols[i]);
				if (inputColType != null) {
					resTypes[resIndex] = inputColType;
				}
			}
		}

		for (int i = 0; i < resNames.length; i++) {
			for (String timeCol : timeCols) {
				if (resNames[i].equals(timeCol)) {
					resTypes[i] = TypeInformation.of(Timestamp.class);
					break;
				}
			}
		}
	}

	static String concatColNames(String[] strings) {
		if (strings == null || strings.length == 0) {
			return null;
		}
		StringBuilder sbd = new StringBuilder();
		boolean firstValue = true;
		for (String s : strings) {
			if (!firstValue) {
				sbd.append(", ");
			}
			sbd.append(s);
			if (firstValue) {
				firstValue = false;
			}
		}
		return sbd.toString();
	}

	public static class PeriodExtendBound
		extends BoundedOutOfOrdernessTimestampExtractor {
		private static final long serialVersionUID = 4066823127559292096L;
		int timeIndex;
		public PeriodExtendBound(Time maxOutOfOrderness, int timeIndex) {
			super(maxOutOfOrderness);
			this.timeIndex = timeIndex;
		}

		@Override
		public long extractTimestamp(Object element) {
			return ((Timestamp) ((Row) element).getField(timeIndex)).getTime();
		}
	}

	public static class PunctuatedAssigner
		implements AssignerWithPunctuatedWatermarks <Row> {
		private static final long serialVersionUID = 2992934406761649522L;
		long lateness;
		int timeIndex;
		private long currentMaxTimestamp = -1;

		PunctuatedAssigner(Time lateness, int timeIndex) {
			this.lateness = lateness.toMilliseconds();
			this.timeIndex = timeIndex;
		}

		@Override
		public long extractTimestamp(Row value, long previousElementTimestamp) {
			long thisTime = ((Timestamp) value.getField(timeIndex)).getTime();
			currentMaxTimestamp = Math.max(thisTime, currentMaxTimestamp);
			return thisTime;
		}

		@Override
		public Watermark checkAndGetNextWatermark(Row value, long extractedTimestamp) {
			return new Watermark(currentMaxTimestamp - lateness);
		}
	}
}

