package com.alibaba.alink.operator.batch.huge.word2vec;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Random;

public class InitVecs extends RichMapPartitionFunction <Row, Tuple2 <Long, float[]>> {
	private static final long serialVersionUID = -5254001954867421910L;
	private final long seed;
	private final int vectorSize;
	private int vocSize;

	public InitVecs(long seed, int vectorSize) {
		this.seed = seed;
		this.vectorSize = vectorSize;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		this.vocSize =
			((Params) getRuntimeContext()
				.getBroadcastVariable("w2vContext").get(0))
				.getLongOrDefault("vocSize", 0L)
				.intValue();
	}

	@Override
	public void mapPartition(Iterable <Row> values, Collector <Tuple2 <Long, float[]>> out)
		throws Exception {
		int pid = getRuntimeContext().getIndexOfThisSubtask();
		Random rand = new Random(this.seed + pid);
		for (Row row : values) {
			Long idx = (Long) row.getField(2);
			out.collect(new Tuple2 <>(idx + this.vocSize, new float[this.vectorSize]));
			float[] ds = new float[this.vectorSize];
			for (int i = 0; i < this.vectorSize; i++) {
				ds[i] = (rand.nextFloat() - 0.5f) / vectorSize;
			}
			out.collect(new Tuple2 <>(idx, ds));
		}
	}
}
