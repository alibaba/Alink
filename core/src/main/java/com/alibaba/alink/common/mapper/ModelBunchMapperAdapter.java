package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.model.ModelSource;

import java.io.Serializable;
import java.util.List;

/**
 * Adapt a {@link ModelMapper} to run within flink.
 * <p>
 * This adapter class hold the target {@link ModelMapper} and it's {@link ModelSource}. Upon open(),
 * it will load model rows from {@link ModelSource} into {@link ModelMapper}.
 */
public class ModelBunchMapperAdapter extends RichMapPartitionFunction <Row, Row> implements Serializable {

	private static final long serialVersionUID = -2288549358571532418L;
	/**
	 * The ModelMapper to adapt.
	 */
	private final ModelMapper mapper;

	/**
	 * Load model data from ModelSource when open().
	 */
	private final ModelSource modelSource;

	private final int bunchSize;

	public ModelBunchMapperAdapter(ModelMapper mapper, ModelSource modelSource, int bunchSize) {
		this.mapper = mapper;
		this.modelSource = modelSource;
		this.bunchSize = bunchSize;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		List <Row> modelRows = this.modelSource.getModelRows(getRuntimeContext());
		this.mapper.loadModel(modelRows);
		this.mapper.open();
	}

	@Override
	public void mapPartition(Iterable <Row> values, Collector <Row> out) throws Exception {
		Row[] bunchRows = new Row[this.bunchSize];
		Row[] outRows = new Row[this.bunchSize];
		int curBunchSize = 0;
		for (Row value : values) {
			if (curBunchSize == this.bunchSize) {
				mapper.map(bunchRows, outRows, this.bunchSize);
				for (Row outRow : outRows) {
					out.collect(outRow);
				}
				bunchRows[0] = value;
				curBunchSize = 1;
			} else {
				bunchRows[curBunchSize] = value;
				curBunchSize++;
			}
		}
		if (curBunchSize > 0) {
			mapper.map(bunchRows, outRows, curBunchSize);
			for (int i = 0; i < curBunchSize; i++) {
				out.collect(outRows[i]);
			}
		}
	}

	@Override
	public void close() throws Exception {
		this.mapper.close();
	}
}
