package com.alibaba.alink.operator.common.tree;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CommunicateFunction;

import java.util.ArrayList;
import java.util.List;

public class Bcast<DT> extends CommunicateFunction {

	private final String bufferName;
	private final int root;
	private final TypeInformation <?> type;

	public Bcast(String bufferName, int root, TypeInformation <?> type) {
		this.bufferName = bufferName;
		this.root = root;
		this.type = type;
	}

	@Override
	public <T> DataSet <T> communicateWith(DataSet <T> input, int sessionId) {
		final String localBufferName = bufferName;
		final int localRoot = root;

		return input
			.mapPartition(new RichMapPartitionFunction <T, Tuple2 <Integer, DT>>() {
				@Override
				public void mapPartition(Iterable <T> values, Collector <Tuple2 <Integer, DT>> out) {
					ComContext context = new ComContext(sessionId, getIterationRuntimeContext());

					if (context.getTaskId() != localRoot) {
						return;
					}

					Iterable <DT> buffer = context.getObj(localBufferName);

					for (DT dt : buffer) {
						for (int i = 0; i < context.getNumTask(); ++i) {
							out.collect(Tuple2.of(i, dt));
						}
					}
				}
			})
			.returns(Types.TUPLE(Types.INT, type))
			.withBroadcastSet(input, "barrier")
			.partitionCustom(new Partitioner <Integer>() {
				@Override
				public int partition(Integer key, int numPartitions) {
					return key;
				}
			}, 0)
			.mapPartition(new RichMapPartitionFunction <Tuple2 <Integer, DT>, T>() {
				@Override
				public void mapPartition(Iterable <Tuple2 <Integer, DT>> values, Collector <T> out) {
					ComContext context = new ComContext(sessionId, getIterationRuntimeContext());

					if (context.getTaskId() == localRoot) {
						return;
					}

					List <DT> buffer = new ArrayList <>();

					for (Tuple2 <Integer, DT> value : values) {
						buffer.add(value.f1);
					}

					context.putObj(localBufferName, buffer);
				}
			})
			.returns(input.getType());
	}
}
