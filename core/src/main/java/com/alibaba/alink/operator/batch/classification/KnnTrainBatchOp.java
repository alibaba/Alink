package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorAssemblerBatchOp;
import com.alibaba.alink.operator.batch.similarity.VectorNearestNeighborTrainBatchOp;
import com.alibaba.alink.params.classification.KnnTrainParams;

/**
 * KNN is to classify unlabeled observations by assigning them to the class of the most similar labeled examples.
 * Note that though there is no ``training process`` in KNN, we create a ``fake one`` to use in pipeline model.
 * In this operator, we do some preparation to speed up the inference process.
 */
public final class KnnTrainBatchOp extends BatchOperator <KnnTrainBatchOp>
	implements KnnTrainParams <KnnTrainBatchOp> {
	private static final long serialVersionUID = -3118065094037473283L;

	private static String VECTOR_COL = "ALINK_VECTOR_COL";

	public KnnTrainBatchOp() {
		this(null);
	}

	public KnnTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public KnnTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		Preconditions.checkArgument(getFeatureCols() == null ^ getVectorCol() == null,
			"Must either set featureCols or vectorCol!");
		BatchOperator in = checkAndGetFirst(inputs);
		if (null != getFeatureCols()) {
			in = new VectorAssemblerBatchOp()
				.setSelectedCols(getFeatureCols())
				.setOutputCol(VECTOR_COL)
				.setReservedCols(getLabelCol())
				.linkFrom(in);
			this.setVectorCol(VECTOR_COL);
		}
		VectorNearestNeighborTrainBatchOp train = new VectorNearestNeighborTrainBatchOp(getParams())
			.setMetric(getDistanceType().name())
			.setSelectedCol(getVectorCol())
			.setIdCol(getLabelCol())
			.linkFrom(in);

		this.setOutput(train.getDataSet(), train.getSchema());
		return this;
	}
}
