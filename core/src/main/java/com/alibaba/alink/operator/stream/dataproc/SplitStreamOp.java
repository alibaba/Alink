package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.dataproc.SplitParams;

import java.util.Random;

/**
 * Split a stream data into two parts.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = {@PortSpec(PortType.DATA), @PortSpec(PortType.DATA)})
@NameCn("数据拆分")
public final class SplitStreamOp extends StreamOperator<SplitStreamOp>
	implements SplitParams<SplitStreamOp> {

	private static final long serialVersionUID = 9032637631974546738L;

	public SplitStreamOp() {
		this(new Params());
	}

	public SplitStreamOp(Params params) {
		super(params);
	}

	public SplitStreamOp(double fraction) {
		this(new Params().set(FRACTION, fraction));
	}

	@Override
	public SplitStreamOp linkFrom(StreamOperator<?>... inputs) {
		StreamOperator<?> in = checkAndGetFirst(inputs);

		SingleOutputStreamOperator<Row> split = in
			.getDataStream().process(new RandomSplit(getFraction()));

		DataStream<Row> partB = split.getSideOutput(RandomSplit.B_TAG);

		this.setOutput(split, in.getSchema());

		this.setSideOutputTables(new Table[]{
			DataStreamConversionUtil.toTable(getMLEnvironmentId(), partB, in.getSchema())});

		return this;
	}

	private static final class RandomSplit extends ProcessFunction<Row, Row> {
		public static final OutputTag<Row> B_TAG = new OutputTag<Row>("b") {
		};

		private double fraction;
		private Random random = null;

		public RandomSplit(double fraction) {
			this.fraction = fraction;
		}

		@Override
		public void processElement(Row row, Context context, Collector<Row> collector) throws Exception {
			if (null == random) {
				random = new Random(System.currentTimeMillis());
			}

			if (random.nextDouble() < fraction) {
				collector.collect(row);
			} else {
				context.output(B_TAG, row);
			}
		}
	}
}
