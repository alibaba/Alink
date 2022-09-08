package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.model.ModelSource;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class ModelBunchMapperAdapterMT extends RichMapPartitionFunction <Row, Row> implements Serializable {

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

	private transient BunchMapperMTWrapper wrapper;

	private final int bunchSize;

	public ModelBunchMapperAdapterMT(ModelMapper mapper, ModelSource modelSource, int numThreads, int bunchSize) {
		this.mapper = mapper;
		this.modelSource = modelSource;
		this.numThreads = numThreads;
		this.bunchSize = bunchSize;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		List <Row> modelRows = this.modelSource.getModelRows(getRuntimeContext());
		this.mapper.loadModel(modelRows);
		this.mapper.open();
		this.wrapper = new BunchMapperMTWrapper(numThreads, () -> this.mapper::bunchMap);
		this.wrapper.open(parameters);
	}

	@Override
	public void close() throws Exception {
		this.wrapper.close();
		this.mapper.close();
		super.close();
	}

	@Override
	public void mapPartition(Iterable <Row> values, Collector <Row> out) throws Exception {
		Row[] bunchRows = new Row[this.bunchSize];
		int curBunchSize = 0;
		for (Row value : values) {
			if (curBunchSize == this.bunchSize) {
				wrapper.flatMap(bunchRows, out);
				bunchRows[0] = value;
				curBunchSize = 1;
			} else {
				bunchRows[curBunchSize] = value;
				curBunchSize++;
			}
		}
		if (curBunchSize > 0) {
			wrapper.flatMap(Arrays.copyOf(bunchRows, curBunchSize), out);
		}
	}
}
