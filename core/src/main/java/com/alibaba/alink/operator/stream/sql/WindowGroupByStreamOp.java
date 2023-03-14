package com.alibaba.alink.operator.stream.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.sql.WindowGroupByParams;

import java.sql.Timestamp;

/**
 * A wrapper of Flink's window groupby.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("SQL操作：WindowGroupBy")
@NameEn("SQL：WindowGroupBy")
public final class WindowGroupByStreamOp extends StreamOperator <WindowGroupByStreamOp>
	implements WindowGroupByParams <WindowGroupByStreamOp> {
	private static final long serialVersionUID = -7231017642191624430L;

	public WindowGroupByStreamOp() {
		this(new Params());
	}

	public WindowGroupByStreamOp(Params params) {
		super(params);
	}

	private static DataStream <Row> getDataStreamWithExplicitTypeDefine(DataStream <Row> data, String[] colNames,
																		TypeInformation <?>[] colTypes) {
		DataStream <Row> r = data
			.map(
				new MapFunction <Row, Row>() {
					private static final long serialVersionUID = 1424209144153966514L;

					@Override
					public Row map(Row t) throws Exception {
						return t;
					}
				}
			)
			.returns(new RowTypeInfo(colTypes, colNames));

		return r;
	}

	String createSelectClause(String[] colNames) {
		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < colNames.length; i++) {
			if (i > 0) {
				sbd.append(",");
			}
			sbd.append(colNames[i]);
		}
		return sbd.toString();
	}

	@Override
	public WindowGroupByStreamOp linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);
		String tmpTableName = StreamOperator.createUniqueTableName();

		MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamTableEnvironment().registerDataStream(tmpTableName,
			getDataStreamWithExplicitTypeDefine(in.getDataStream(), in.getColNames(), in.getColTypes()),
			createSelectClause(in.getColNames()) + ",proctime.proctime");

		String windowSpec = null;
		WindowGroupByParams.WindowType windowType = getWindowType();
		String windowUnit = getIntervalUnit().toString();
		switch (windowType) {
			case TUMBLE: {
				int windowLength = getWindowLength();
				windowSpec = String.format("proctime, INTERVAL '%d' %s", windowLength, windowUnit);
				break;
			}
			case HOP: {
				int windowLength = getWindowLength();
				int sliddingLength = getSlidingLength();
				windowSpec = String.format("proctime, INTERVAL '%d' %s, INTERVAL '%d' %s", sliddingLength, windowUnit,
					windowLength, windowUnit);
				break;
			}
			case SESSION: {
				int sessionGap = getSessionGap();
				windowSpec = String.format("proctime, INTERVAL '%d' %s", sessionGap, windowUnit);
				break;
			}
			default: {
				throw new IllegalArgumentException("invalid window type: " + windowType);
			}
		}

		String selectClause = getSelectClause();
		String groupByClause = getGroupByClause();
		boolean hasGroupByKey = !StringUtils.isNullOrWhitespaceOnly(groupByClause);

		String cmd = String.format(
			"SELECT %s, CAST((%s_start(%s)) as TIMESTAMP(3)) as window_start, CAST((%s_end(%s)) as TIMESTAMP(3)) as "
				+ "window_end FROM %s GROUP BY %s(%s)",
			selectClause, windowType, windowSpec, windowType, windowSpec, tmpTableName, windowType, windowSpec);
		if (hasGroupByKey) {
			cmd = cmd + String.format(", %s", groupByClause);
		}

		try {
			StreamOperator output = MLEnvironmentFactory.get(in.getMLEnvironmentId()).streamSQL(cmd)
				.setMLEnvironmentId(getMLEnvironmentId());
			this.setOutputTable(output.getOutputTable());
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid input: " + cmd + ", because: " + e);
		}

		final int winStartCol = TableUtil.findColIndexWithAssertAndHint(
			this.getOutputTable().getSchema().getFieldNames(), "window_start");
		final int winEndCol = TableUtil.findColIndexWithAssertAndHint(this.getOutputTable().getSchema()
				.getFieldNames(),
			"window_end");

		// fix the time zone of window_start and window_end
		DataStream <Row> rows = this.getDataStream();
		rows = rows.map(new RichMapFunction <Row, Row>() {
			private static final long serialVersionUID = -7654428798338632450L;

			@Override
			public Row map(Row value) throws Exception {
				Timestamp startTime = (Timestamp) value.getField(winStartCol);
				Timestamp endTime = (Timestamp) value.getField(winEndCol);
				long currTimeMs = System.currentTimeMillis();
				long startTimeMs = startTime.getTime();
				long endTimeMs = endTime.getTime();
				long offset = currTimeMs - endTimeMs;
				long offsetHour = Math.round((double) offset / (1000L * 3600L));
				offset = offsetHour * 1000L * 3600L;
				value.setField(winStartCol, new Timestamp(startTimeMs + offset));
				value.setField(winEndCol, new Timestamp(endTimeMs + offset));
				return value;
			}
		}).name("correct_window_timezone");

		setOutput(rows, this.getOutputTable().getSchema());
		return this;
	}
}
