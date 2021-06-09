package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.comqueue.IterTaskObjKeeper;

import java.io.Serializable;

/**
 * Adapter for a {@link ModelMapper}, which loads model by {@link IterableModelLoader}.
 * This adapter class hold the handler of the state. Upon open(), it will load model
 * rows from {@link IterTaskObjKeeper} into {@link ModelMapper}.
 */
public class IterableModelLoaderModelMapperAdapter extends RichMapFunction <Row, Row> implements Serializable {

	private static final long serialVersionUID = 4280385934396641248L;
	private ModelMapper iterableModelMapper;
	private long handler;

	public IterableModelLoaderModelMapperAdapter(long handler) {
		this.handler = handler;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		for (int i = 0; i < getRuntimeContext().getNumberOfParallelSubtasks(); ++i) {
			if (IterTaskObjKeeper.contains(handler, i)) {
				iterableModelMapper = IterTaskObjKeeper.get(handler, i);
				break;
			}
		}
		assert null != iterableModelMapper;
		iterableModelMapper.open();
	}

	@Override
	public void close() throws Exception {
		iterableModelMapper.close();
		super.close();
	}

	@Override
	public Row map(Row value) throws Exception {
		return iterableModelMapper.map(value);
	}
}
