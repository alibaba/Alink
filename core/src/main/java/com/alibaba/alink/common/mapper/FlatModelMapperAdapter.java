package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
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
public class FlatModelMapperAdapter extends RichFlatMapFunction <Row, Row> implements Serializable {

	private static final long serialVersionUID = 6773888244253890717L;
	private final FlatModelMapper mapper;
	private final ModelSource modelSource;

	public FlatModelMapperAdapter(FlatModelMapper mapper, ModelSource modelSource) {
		this.mapper = mapper;
		this.modelSource = modelSource;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		List <Row> modelRows = this.modelSource.getModelRows(getRuntimeContext());
		this.mapper.loadModel(modelRows);
		this.mapper.open();
	}

	@Override
	public void flatMap(Row value, Collector <Row> out) throws Exception {
		this.mapper.flatMap(value, out);
	}

	@Override
	public void close() throws Exception {
		this.mapper.close();
	}
}
