package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * Adapt a {@link Mapper} to run within flink.
 */
public class FlatMapperAdapterMT extends RichFlatMapFunction <Row, Row> implements Serializable {

	private static final long serialVersionUID = -8280909075523296990L;
	private final FlatMapper flatMapper;

	private final int numThreads;

	private transient FlatMapperMTWrapper wrapper;

	public FlatMapperAdapterMT(FlatMapper flatMapper, int numThreads) {
		this.flatMapper = flatMapper;
		this.numThreads = numThreads;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.flatMapper.open();
		this.wrapper = new FlatMapperMTWrapper(numThreads, () -> this.flatMapper);
		this.wrapper.open(parameters);
	}

	@Override
	public void close() throws Exception {
		this.wrapper.close();
		this.flatMapper.close();
		super.close();
	}

	@Override
	public void flatMap(Row value, Collector <Row> out) throws Exception {
		this.wrapper.flatMap(value, out);
	}
}
