package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.mapper.MapperMTWrapper;
import com.alibaba.alink.common.model.ModelSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

/**
 * Multi-threaded version.
 * Adapt a {@link RecommKernel} to run within flink.
 * <p>
 * This adapter class hold the target {@link RecommKernel} and it's {@link ModelSource}. Upon open(),
 * it will load model rows from {@link ModelSource} into {@link RecommKernel}.
 */
public class RecommAdapterMT extends RichFlatMapFunction <Row, Row> implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(MapperMTWrapper.class);
	private static final long serialVersionUID = -5842480394711815042L;

	private final RecommKernel recommKernel;

	/**
	 * Load model data from ModelSource when open().
	 */
	private final ModelSource modelSource;

	private final int numThreads;

	private transient MapperMTWrapper wrapper;

	public RecommAdapterMT(RecommKernel recommKernel, ModelSource modelSource, int numThreads) {
		this.recommKernel = recommKernel;
		this.modelSource = modelSource;
		this.numThreads = numThreads;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		LOG.info("start loading model");

		List <Row> modelRows = this.modelSource.getModelRows(getRuntimeContext());
		this.recommKernel.loadModel(modelRows);

		this.wrapper = new MapperMTWrapper(numThreads, () -> this.recommKernel.mirror()::recommend);
		this.wrapper.open(parameters);
	}

	@Override
	public void close() throws Exception {
		this.wrapper.close();
	}

	@Override
	public void flatMap(Row row, Collector <Row> collector) throws Exception {
		this.wrapper.flatMap(row, collector);
	}
}
