package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.model.ModelSource;

import java.io.Serializable;
import java.util.List;

/**
 * Adapt a {@link RecommKernel} to run within flink.
 * <p>
 * This adapter class hold the target {@link RecommKernel} and it's {@link ModelSource}. Upon open(),
 * it will load model rows from {@link ModelSource} into {@link RecommKernel}.
 */
public class RecommAdapter extends RichMapFunction <Row, Row> implements Serializable {

	private static final long serialVersionUID = 1074780682017058609L;
	private final RecommKernel recommKernel;
	private transient long numInputRecords;

	/**
	 * Load model data from ModelSource when open().
	 */
	private final ModelSource modelSource;

	public RecommAdapter(RecommKernel recommKernel, ModelSource modelSource) {
		this.recommKernel = recommKernel;
		this.modelSource = modelSource;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		List <Row> modelRows = this.modelSource.getModelRows(getRuntimeContext());
		this.recommKernel.loadModel(modelRows);
		this.numInputRecords = 0L;
	}

	@Override
	public void close() throws Exception {
		super.close();
	}

	@Override
	public Row map(Row row) throws Exception {
		this.numInputRecords++;
		return this.recommKernel.recommend(row);
	}
}
