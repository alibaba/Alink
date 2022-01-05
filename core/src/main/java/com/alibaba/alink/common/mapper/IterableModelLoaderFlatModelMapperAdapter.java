package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.comqueue.IterTaskObjKeeper;

import java.io.Serializable;

/**
 * Adapter for a {@link FlatModelMapper}, which loads model by {@link IterableModelLoader}. This adapter class hold the
 * handler of the state. Upon open(), it will load model rows from {@link IterTaskObjKeeper} into {@link
 * FlatModelMapper}.
 */
public class IterableModelLoaderFlatModelMapperAdapter extends RichFlatMapFunction <Row, Row> implements Serializable {

	private static final long serialVersionUID = 4280385934396641248L;
	private FlatModelMapper iterableFlatModelMapper;
	private final long handler;

	public IterableModelLoaderFlatModelMapperAdapter(long handler) {
		this.handler = handler;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		for (int i = 0; i < getRuntimeContext().getNumberOfParallelSubtasks(); ++i) {
			iterableFlatModelMapper = IterTaskObjKeeper.containsAndRemoves(handler, i);
			if (null != iterableFlatModelMapper) {
				break;
			}
		}
		assert null != iterableFlatModelMapper;
		iterableFlatModelMapper.open();
	}

	@Override
	public void close() throws Exception {
		iterableFlatModelMapper.close();
		super.close();
	}

	@Override
	public void flatMap(Row value, Collector <Row> out) throws Exception {
		iterableFlatModelMapper.flatMap(value, out);
	}
}
