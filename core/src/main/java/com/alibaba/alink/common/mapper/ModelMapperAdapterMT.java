package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.model.ModelSource;

import java.io.Serializable;
import java.util.List;

public class ModelMapperAdapterMT extends RichFlatMapFunction <Row, Row> implements Serializable {

	private static final long serialVersionUID = -2808807375955173295L;
	/**
	 * The ModelMapper to adapt.
	 */
	private final ModelMapper mapper;

	/**
	 * Load model data from ModelSource when open().
	 */
	private final ModelSource modelSource;

	private final int numThreads;

	private transient MapperMTWrapper wrapper;

	public ModelMapperAdapterMT(ModelMapper mapper, ModelSource modelSource, int numThreads) {
		this.mapper = mapper;
		this.modelSource = modelSource;
		this.numThreads = numThreads;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		List <Row> modelRows = this.modelSource.getModelRows(getRuntimeContext());
		this.mapper.loadModel(modelRows);

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
