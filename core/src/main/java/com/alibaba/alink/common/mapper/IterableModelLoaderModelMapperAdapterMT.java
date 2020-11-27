package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.comqueue.IterTaskObjKeeper;

import java.io.Serializable;

/**
 * Adapter for a {@link ModelMapper}, which loads model by {@link IterableModelLoader}.
 * This adapter class hold the handler of the state. Upon open(), it will load model
 * rows from {@link IterTaskObjKeeper} into {@link ModelMapper}.
 */

public class IterableModelLoaderModelMapperAdapterMT extends RichFlatMapFunction <Row, Row> implements Serializable {

	private static final long serialVersionUID = -727513055106586691L;
	private ModelMapper iterableModelMapper;
	private long handler;
	private final int numThreads;
	private transient MapperMTWrapper wrapper;

	public IterableModelLoaderModelMapperAdapterMT(long handler, int numThreads) {
		this.handler = handler;
		this.numThreads = numThreads;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		for (int i = 0; i < getRuntimeContext().getNumberOfParallelSubtasks(); ++i) {
			if(IterTaskObjKeeper.contains(handler, i)){
				iterableModelMapper = IterTaskObjKeeper.get(handler, i);
				break;
			}
		}
		assert null != iterableModelMapper;
		this.wrapper = new MapperMTWrapper(numThreads, () -> this.iterableModelMapper.mirror()::map);
		this.wrapper.open(parameters);
	}

	@Override
	public void close() throws Exception {
		this.wrapper.close();
		this.iterableModelMapper.close();
		super.close();
	}

	@Override
	public void flatMap(Row value, Collector <Row> out) throws Exception {
		this.wrapper.flatMap(value, out);
	}
}
