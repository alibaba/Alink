package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.stream.StreamOperator;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Print Stream op to screen.
 */
@IoOpAnnotation(name = "print", ioType = IOType.SinkStream)
@InputPorts(values = {@PortSpec(PortType.ANY)})
@OutputPorts()
@NameCn("流式数据打印")
public class PrintStreamOp extends StreamOperator <PrintStreamOp> {
	@NameCn("刷新间隔")
	@DescCn("输出的刷新间隔")
	public static final ParamInfo <Integer> REFRESH_INTERVAL = ParamInfoFactory
		.createParamInfo("refreshInterval", Integer.class)
		.setDescription("refresh interval")
		.setHasDefaultValue(-1)
		.build();

	@NameCn("每个窗口内的最大输出条数")
	@DescCn("每个窗口内的最大输出条数")
	public static final ParamInfo <Integer> MAX_LIMIT = ParamInfoFactory
		.createParamInfo("maxLimit", Integer.class)
		.setDescription("max limit")
		.setHasDefaultValue(100)
		.build();

	private static final long serialVersionUID = -7482957550550215050L;

	public PrintStreamOp() {
		this(null);
	}

	public PrintStreamOp(Params params) {
		super(params);
	}

	public static void setStreamPrintStream(PrintStream printStream) {
		System.setErr(printStream);
	}

	@Override
	public PrintStreamOp linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);
		try {
			System.err.println(TableUtil.formatTitle(in.getColNames()));
			final int refreshInterval = getParams().get(REFRESH_INTERVAL);
			if (refreshInterval <= 0) {
				DataStreamConversionUtil.fromTable(getMLEnvironmentId(), in.getOutputTable()).addSink(
					new StreamPrintSinkFunction());
			} else {
				final int maxLimit = getParams().get(MAX_LIMIT);
				DataStreamConversionUtil.fromTable(getMLEnvironmentId(), in.getOutputTable())
					.timeWindowAll(Time.of(refreshInterval, TimeUnit.SECONDS))
					.apply(new AllWindowFunction <Row, List <Row>, TimeWindow>() {
						private static final long serialVersionUID = -5002192700679782400L;

						@Override
						public void apply(TimeWindow window, Iterable <Row> values, Collector <List <Row>> out) {
							List <Row> list = new ArrayList <>();
							for (Row row : values) {
								if (list.size() < maxLimit) {
									list.add(row);
								} else {
									break;
								}
							}
							out.collect(list);
						}
					}).addSink(new PrintStreamOp.StreamPrintListRowSinkFunction());
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		this.setOutputTable(in.getOutputTable());
		return this;
	}

	public static class StreamPrintListRowSinkFunction extends RichSinkFunction <List <Row>> {
		private static final long serialVersionUID = 1L;
		private transient PrintStream stream;

		public StreamPrintListRowSinkFunction() {
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			this.stream = System.err;
		}

		@Override
		public void invoke(List <Row> records) {
			for (Row record : records) {
				this.stream.println(TableUtil.formatRows(record));
			}
		}

		@Override
		public void close() {
			this.stream = null;
		}

		@Override
		public String toString() {
			return "Print to " + this.stream.toString();
		}
	}

	public static class StreamPrintSinkFunction extends RichSinkFunction <Row> {
		private static final long serialVersionUID = 1L;
		private transient PrintStream stream;

		public StreamPrintSinkFunction() {
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			this.stream = System.err;
		}

		@Override
		public void invoke(Row record) {
			this.stream.println(TableUtil.formatRows(record));
		}

		@Override
		public void close() {
			this.stream = null;
		}

		@Override
		public String toString() {
			return "Print to " + this.stream.toString();
		}
	}
}

