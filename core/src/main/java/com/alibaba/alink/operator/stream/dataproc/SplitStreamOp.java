package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.dataproc.SplitParams;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Split a stream data into two parts.
 */
public final class SplitStreamOp extends StreamOperator<SplitStreamOp>
	implements SplitParams <SplitStreamOp> {

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
		class RandomSelectorOp implements OutputSelector <Row> {
			private double fraction;
			private Random random = null;

			public RandomSelectorOp(double fraction) {
				this.fraction = fraction;
			}

			@Override
			public Iterable <String> select(Row value) {
				if (null == random) {
					random = new Random(System.currentTimeMillis());
				}
				List <String> output = new ArrayList <String>(1);
				output.add((random.nextDouble() < fraction ? "a" : "b"));
				return output;
			}
		}

		SplitStream <Row> splited = in.getDataStream().split(new RandomSelectorOp(getFraction()));
		DataStream <Row> partA = splited.select("a");
		DataStream <Row> partB = splited.select("b");

		this.setOutput(partA, in.getSchema());
		this.setSideOutputTables(new Table[]{
		DataStreamConversionUtil.toTable(getMLEnvironmentId(), partB, in.getSchema())});

		return this;
	}

}
