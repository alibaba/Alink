package com.alibaba.alink.operator.batch.source;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.RandomVector;
import com.alibaba.alink.params.io.RandomVectorSourceBatchParams;

/**
 * Generate vector with random values.
 */
@IoOpAnnotation(name = "random_vector", ioType = IOType.SourceBatch)
public final class RandomVectorSourceBatchOp extends BaseSourceBatchOp <RandomVectorSourceBatchOp>
	implements RandomVectorSourceBatchParams <RandomVectorSourceBatchOp> {

	private static final long serialVersionUID = 392478571040718084L;

	public RandomVectorSourceBatchOp() {
		this(new Params());
	}

	public RandomVectorSourceBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(RandomVectorSourceBatchOp.class), params);
	}

	@Override
	public Table initializeDataSource() {

		int numRows = getNumRows();
		Integer[] size = getSize();
		String idColName = this.getIdCol();
		double sparsity = getSparsity();

		String[] keepColNames = idColName != null ? new String[] {idColName} : new String[] {};

		String outputColName = getOutputCol();

		BatchOperator<?> initData = idColName != null ? new NumSeqSourceBatchOp(0, numRows - 1, idColName, getParams())
			: new NumSeqSourceBatchOp(0L, numRows - 1, getParams());

		return initData
			.udtf(idColName, new String[] {outputColName},
				new RandomVector(size, sparsity), keepColNames)
			.getOutputTable();
	}
}
