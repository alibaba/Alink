package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * An {@link RichSinkFunction} which swallows all.
 *
 * @param <T>
 */
public class DummyTableSink<T> extends RichSinkFunction <T> {
	private static final long serialVersionUID = -3444483615651991551L;

	@Override
	public void invoke(T value) throws Exception {

	}
}
