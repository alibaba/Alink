package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * Adapt a {@link Mapper} to run within flink.
 */
public class MapperAdapterMT extends RichFlatMapFunction <Row, Row> implements Serializable {

	private static final long serialVersionUID = -6526334200739568501L;
	private final Mapper mapper;

	private final int numThreads;

	private transient MapperMTWrapper wrapper;

	public MapperAdapterMT(Mapper mapper, int numThreads) {
		this.mapper = mapper;
		this.numThreads = numThreads;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.mapper.open();
		this.wrapper = new MapperMTWrapper(numThreads, () -> this.mapper.mirror()::map);
		this.wrapper.open(parameters);
	}

	@Override
	public void close() throws Exception {
		this.wrapper.close();
		this.mapper.close();
		super.close();
	}

	@Override
	public void flatMap(Row value, Collector <Row> out) throws Exception {
		this.wrapper.flatMap(value, out);
	}
}
