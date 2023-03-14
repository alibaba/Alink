package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.FeatureColsVectorColMutexRule;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorAssemblerBatchOp;
import com.alibaba.alink.operator.batch.similarity.VectorNearestNeighborTrainBatchOp;
import com.alibaba.alink.params.classification.KnnTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * KNN is to classify unlabeled observations by assigning them to the class of the most similar labeled examples.
 * Note that though there is no ``training process`` in KNN, we create a ``fake one`` to use in pipeline model.
 * In this operator, we do some preparation to speed up the inference process.
 */
@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@FeatureColsVectorColMutexRule
@ParamSelectColumnSpec(name = "labelCol")
@NameCn("最近邻分类训练")
@NameEn("Knn Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.classification.KnnClassifier")
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
		AkPreconditions.checkArgument(getFeatureCols() == null ^ getVectorCol() == null,
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
