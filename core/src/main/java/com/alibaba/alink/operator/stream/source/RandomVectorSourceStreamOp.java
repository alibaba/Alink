package com.alibaba.alink.operator.stream.source;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.probabilistic.XRandom;
import com.alibaba.alink.operator.common.dataproc.RandomVector;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.RandomVectorSourceStreamParams;

/**
 * Generate vector with random values.
 */

@IoOpAnnotation(name = "random_vector", ioType = IOType.SourceStream)
public final class RandomVectorSourceStreamOp extends BaseSourceStreamOp <RandomVectorSourceStreamOp>
	implements RandomVectorSourceStreamParams <RandomVectorSourceStreamOp> {

	private static final long serialVersionUID = -2004518005886439388L;
	private XRandom rd = new XRandom();

	public RandomVectorSourceStreamOp() {
		this(null);
	}

	public RandomVectorSourceStreamOp(long maxRows, int[] size, double sparsity) {
		this(new Params()
			.set(SIZE, convert(size))
			.set(TIME_PER_SAMPLE, 1.0)
			.set(MAX_ROWS, maxRows)
			.set(SPARSITY, sparsity));
	}

	public RandomVectorSourceStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(RandomVectorSourceStreamOp.class), params);
	}

	private static Integer[] convert(int[] size) {
		Integer[] re = new Integer[size.length];
		for (int i = 0; i < size.length; i++) {
			re[i] = size[i];
		}
		return re;
	}

	@Override
	public Table initializeDataSource() {
		long maxRows = getMaxRows();
		Integer[] size = getSize();
		String idColName = getIdCol();
		double sparsity = getSparsity();
		Double timePerSample = getTimePerSample();
		String[] keepColNames = idColName != null ? new String[] {idColName} : new String[] {};

		String outputColName = getOutputCol();

		StreamOperator<?> init_data;
		if (timePerSample != null && idColName != null) {
			init_data = new NumSeqSourceStreamOp(1, maxRows, idColName, timePerSample, getParams());
		} else if (timePerSample != null && idColName == null) {
			init_data = new NumSeqSourceStreamOp(1, maxRows, timePerSample, getParams());
		} else if (timePerSample == null && idColName != null) {
			init_data = new NumSeqSourceStreamOp(1, maxRows, idColName, getParams());
		} else {
			init_data = new NumSeqSourceStreamOp(1, maxRows, getParams());
		}

		return init_data
			.udtf(idColName, new String[] {outputColName},
				new RandomVector(size, sparsity), keepColNames)
			.getOutputTable();
	}
}
