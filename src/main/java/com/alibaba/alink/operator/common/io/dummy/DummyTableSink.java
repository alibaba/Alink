package com.alibaba.alink.operator.common.io.dummy;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * An {@link RichSinkFunction} which swallows all.
 * @param <T>
 */
public class DummyTableSink<T> extends RichSinkFunction<T> {
	@Override
	public void invoke(T value) throws Exception {

	}
}
