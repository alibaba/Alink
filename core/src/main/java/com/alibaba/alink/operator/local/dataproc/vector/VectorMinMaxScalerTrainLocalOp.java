package com.alibaba.alink.operator.local.dataproc.vector;

import org.apache.flink.api.java.tuple.Tuple3;
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
import com.alibaba.alink.operator.common.dataproc.vector.VectorMinMaxScalerModelDataConverter;
import com.alibaba.alink.operator.common.dataproc.vector.VectorMinMaxScalerModelInfo;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.lazy.WithModelInfoLocalOp;
import com.alibaba.alink.params.dataproc.vector.VectorMinMaxScalerTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

import static com.alibaba.alink.operator.local.dataproc.vector.VectorStandardScalerTrainLocalOp.calcVectorSRT;

/**
 * MinMaxScaler transforms a dataSet of rows, rescaling each feature
 * to a specific range [min, max). (often [0, 1]).
 * MinMaxScalerTrain will train a model.
 */
@InputPorts(values = @PortSpec(value = PortType.DATA))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量归一化训练")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.dataproc.vector.VectorMinMaxScaler")
public final class VectorMinMaxScalerTrainLocalOp extends LocalOperator <VectorMinMaxScalerTrainLocalOp>
	implements VectorMinMaxScalerTrainParams <VectorMinMaxScalerTrainLocalOp>,
	WithModelInfoLocalOp <VectorMinMaxScalerModelInfo, VectorMinMaxScalerTrainLocalOp,
			VectorMinMaxScalerModelInfoLocalOp> {

	public VectorMinMaxScalerTrainLocalOp() {
		this(new Params());
	}

	public VectorMinMaxScalerTrainLocalOp(Params params) {
		super(params);
	}

	@Override
	public VectorMinMaxScalerTrainLocalOp linkFrom(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
		String vectorColName = getSelectedCol();

		VectorMinMaxScalerModelDataConverter converter = new VectorMinMaxScalerModelDataConverter();
		converter.vectorColName = vectorColName;

		BaseVectorSummary srt = calcVectorSRT(in, vectorColName);

		RowCollector rowCollector = new RowCollector();
		converter.save(new Tuple3 <>(getMin(), getMax(), srt), rowCollector);

		this.setOutputTable(new MTable(rowCollector.getRows(), converter.getModelSchema()));

		return this;
	}

	@Override
	public VectorMinMaxScalerModelInfoLocalOp getModelInfoLocalOp() {
		return new VectorMinMaxScalerModelInfoLocalOp(this.getParams()).linkFrom(this);
	}

}
