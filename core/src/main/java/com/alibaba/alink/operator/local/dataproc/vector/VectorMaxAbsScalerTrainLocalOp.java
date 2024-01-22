package com.alibaba.alink.operator.local.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.operator.common.dataproc.vector.VectorMaxAbsScalarModelInfo;
import com.alibaba.alink.operator.common.dataproc.vector.VectorMaxAbsScalerModelDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.lazy.WithModelInfoLocalOp;
import com.alibaba.alink.params.dataproc.vector.VectorMaxAbsScalerTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

import static com.alibaba.alink.operator.local.dataproc.vector.VectorStandardScalerTrainLocalOp.calcVectorSRT;

/**
 * MaxAbsScaler transforms a dataSet of Vector rows, rescaling each feature to range
 * [-1, 1] by dividing through the maximum absolute value in each feature.
 * MaxAbsScalerTrain will train a model.
 */
@InputPorts(values = @PortSpec(value = PortType.DATA))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量绝对值最大化训练")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.dataproc.vector.VectorMaxAbsScaler")
public final class VectorMaxAbsScalerTrainLocalOp extends LocalOperator <VectorMaxAbsScalerTrainLocalOp>
	implements VectorMaxAbsScalerTrainParams <VectorMaxAbsScalerTrainLocalOp>,
	WithModelInfoLocalOp <VectorMaxAbsScalarModelInfo, VectorMaxAbsScalerTrainLocalOp,
		VectorMaxAbsScalerModelInfoLocalOp> {

	public VectorMaxAbsScalerTrainLocalOp() {
		this(new Params());
	}

	public VectorMaxAbsScalerTrainLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
		String vectorColName = getSelectedCol();

		VectorMaxAbsScalerModelDataConverter converter = new VectorMaxAbsScalerModelDataConverter();
		converter.vectorColName = vectorColName;

		BaseVectorSummary srt = calcVectorSRT(in, vectorColName);

		RowCollector rowCollector = new RowCollector();
		converter.save(srt, rowCollector);

		this.setOutputTable(new MTable(rowCollector.getRows(), converter.getModelSchema()));
	}

	@Override
	public VectorMaxAbsScalerModelInfoLocalOp getModelInfoLocalOp() {
		return new VectorMaxAbsScalerModelInfoLocalOp(this.getParams()).linkFrom(this);
	}

}
