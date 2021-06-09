package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.model.ModelSource;

import java.io.Serializable;
import java.util.List;

/**
 * Adapt a {@link RecommMapper} to run within flink.
 * <p>
 * This adapter class hold the target {@link RecommMapper} and it's {@link ModelSource}. Upon open(), it will load model
 * rows from {@link ModelSource} into {@link RecommMapper}.
 */
public class RecommAdapter extends RichMapFunction <Row, Row> implements Serializable {

	private static final long serialVersionUID = 1074780682017058609L;
	private final RecommMapper recommMapper;
	private transient long numInputRecords;

	/**
	 * Load model data from ModelSource when open().
	 */
	private final ModelSource modelSource;

	public RecommAdapter(RecommMapper recommMapper, ModelSource modelSource) {
		this.recommMapper = recommMapper;
		this.modelSource = modelSource;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		List <Row> modelRows = this.modelSource.getModelRows(getRuntimeContext());
		this.recommMapper.loadModel(modelRows);
		this.numInputRecords = 0L;
	}

	@Override
	public void close() throws Exception {
		super.close();
	}

	@Override
	public Row map(Row row) throws Exception {
		this.numInputRecords++;
		return  recommMapper.map(row);
	}
}
