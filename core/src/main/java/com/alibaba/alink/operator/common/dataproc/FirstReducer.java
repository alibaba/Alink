package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.Internal;

@Internal
public class FirstReducer<T> implements GroupReduceFunction <T, T>, GroupCombineFunction <T, T> {
	private static final long serialVersionUID = 1L;

	private final int count;

	public FirstReducer(int n) {
		count = n;
	}

	@Override
	public void reduce(Iterable <T> values, Collector <T> out) throws Exception {
		int emitCnt = 0;
		for (T val : values) {
			if (emitCnt < count) {
				out.collect(val);
				emitCnt++;
			}
		}
	}

	@Override
	public void combine(Iterable <T> values, Collector <T> out) throws Exception {
		reduce(values, out);
	}
}
