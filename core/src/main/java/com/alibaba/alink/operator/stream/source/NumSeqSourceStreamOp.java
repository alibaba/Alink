package com.alibaba.alink.operator.stream.source;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import org.apache.commons.math3.random.RandomDataGenerator;

import static java.lang.Thread.sleep;

/**
 * Stream sources that represents a range of integers.
 */
@NameCn("数值队列数据源")
@InputPorts
@OutputPorts(values = @PortSpec(PortType.DATA))
public final class NumSeqSourceStreamOp extends BaseSourceStreamOp <NumSeqSourceStreamOp> {

	private static final long serialVersionUID = -1132356020317225421L;

	private final long from;
	private final long to;
	private final String colName;
	private double timePerSample = -1;
	private Double[] timeZones;

	public NumSeqSourceStreamOp(long n) {
		this(1L, n);
	}

	public NumSeqSourceStreamOp(long from, long to) {
		this(from, to, new Params());
	}

	public NumSeqSourceStreamOp(long from, long to, Params params) {
		this(from, to, "num", params);
	}

	public NumSeqSourceStreamOp(long from, long to, String colName) {
		this(from, to, colName, new Params());
	}

	public NumSeqSourceStreamOp(long from, long to, String colName, Params params) {
		super(AnnotationUtils.annotatedName(NumSeqSourceStreamOp.class), params);
		this.from = from;
		this.to = to;
		this.colName = colName;
	}

	public NumSeqSourceStreamOp(long from, long to, double timePerSample) {
		this(from, to, timePerSample, null);
	}

	public NumSeqSourceStreamOp(long from, long to, double timePerSample, Params params) {
		this(from, to, "num", timePerSample, params);
	}

	public NumSeqSourceStreamOp(long from, long to, String colName, double timePerSample) {
		this(from, to, colName, timePerSample, null);
	}

	public NumSeqSourceStreamOp(long from, long to, String colName, double timePerSample, Params params) {
		super(AnnotationUtils.annotatedName(NumSeqSourceStreamOp.class), params);
		this.from = from;
		this.to = to;
		this.colName = colName;
		this.timePerSample = timePerSample;

	}

	public NumSeqSourceStreamOp(long from, long to, Double[] timeZones) {
		this(from, to, "num", timeZones, null);
	}

	public NumSeqSourceStreamOp(long from, long to, Double[] timeZones, Params params) {
		this(from, to, "num", timeZones, params);
	}

	public NumSeqSourceStreamOp(long from, long to, String colName, Double[] timeZones) {
		this(from, to, colName, timeZones, null);
	}

	public NumSeqSourceStreamOp(long from, long to, String colName, Double[] timeZones, Params params) {
		super(AnnotationUtils.annotatedName(NumSeqSourceStreamOp.class), params);
		this.from = from;
		this.to = to;
		this.colName = colName;
		this.timeZones = timeZones;
	}

	@Override
	protected Table initializeDataSource() {
		DataStreamSource <Long> seq
			= MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamExecutionEnvironment()
			.generateSequence(from, to);
		DataStream <Long> data;
		if (timeZones == null && timePerSample == -1) {
			data = seq;
		} else if (timeZones != null && timePerSample == -1) {
			data = seq.map(new SpeedController(timeZones));
		} else {
			data = seq.map(new SpeedController(new Double[] {timePerSample}));
		}
		return MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamTableEnvironment()
			.fromDataStream(data, colName);
	}

	public static class SpeedController extends AbstractRichFunction
		implements MapFunction <Long, Long> {

		private static final long serialVersionUID = 4030421619547107931L;

		RandomDataGenerator rd = new RandomDataGenerator();

		boolean updateSeed = false;
		int numWorker;
		private Double timePerSample;
		private Double[] timeZones;

		public SpeedController(Double[] timeZones) {
			if (timeZones.length == 1) {
				this.timePerSample = timeZones[0];
			} else {
				this.timeZones = timeZones;
			}
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			this.numWorker = getRuntimeContext().getNumberOfParallelSubtasks();
		}

		@Override
		public Long map(Long value) throws Exception {

			if (!updateSeed) {
				rd.reSeed(value);
				updateSeed = true;
			}

			long sleepMs;
			if (timeZones == null) {
				sleepMs = Math.round(1000 * timePerSample * this.numWorker);
			} else if (timeZones.length == 2) {
				sleepMs = Math.round(
					1000 * (timeZones[0] + 0.01 * rd.nextInt(0, 100) * (timeZones[1] - timeZones[0])) * this.numWorker
				);
			} else {
				throw new IllegalArgumentException("time parameter is wrong!");
			}
			sleep(sleepMs);
			return value;
		}
	}

}
