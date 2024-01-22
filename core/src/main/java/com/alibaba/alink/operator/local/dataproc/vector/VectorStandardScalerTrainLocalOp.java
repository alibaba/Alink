package com.alibaba.alink.operator.local.dataproc.vector;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.dataproc.vector.VectorStandardScalerModelDataConverter;
import com.alibaba.alink.operator.common.dataproc.vector.VectorStandardScalerModelInfo;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummarizer;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.operator.common.statistics.basicstatistic.DenseVectorSummarizer;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.lazy.WithModelInfoLocalOp;
import com.alibaba.alink.params.dataproc.vector.VectorStandardTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * StandardScaler transforms a dataSet, normalizing each feature to have unit standard deviation and/or zero mean.
 * If withMean is false, set mean as 0; if withStd is false, set std as 1.
 */
@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量标准化训练")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.dataproc.vector.VectorStandardScaler")
public final class VectorStandardScalerTrainLocalOp extends LocalOperator <VectorStandardScalerTrainLocalOp>
	implements VectorStandardTrainParams <VectorStandardScalerTrainLocalOp>,
	WithModelInfoLocalOp <VectorStandardScalerModelInfo, VectorStandardScalerTrainLocalOp,
		VectorStandardScalerModelInfoLocalOp> {

	public VectorStandardScalerTrainLocalOp() {
		this(new Params());
	}

	public VectorStandardScalerTrainLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
		String vectorColName = getSelectedCol();

		VectorStandardScalerModelDataConverter converter = new VectorStandardScalerModelDataConverter();
		converter.vectorColName = vectorColName;

		BaseVectorSummary srt = calcVectorSRT(in, vectorColName);

		RowCollector rowCollector = new RowCollector();
		converter.save(new Tuple3 <>(getWithMean(), getWithStd(), srt), rowCollector);

		this.setOutputTable(new MTable(rowCollector.getRows(), converter.getModelSchema()));
	}

	@Override
	public VectorStandardScalerModelInfoLocalOp getModelInfoLocalOp() {
		return new VectorStandardScalerModelInfoLocalOp(this.getParams()).linkFrom(this);
	}

	static BaseVectorSummary calcVectorSRT(LocalOperator <?> in, String vectorColName) {
		int idx = TableUtil.findColIndexWithAssertAndHint(in.getSchema(), vectorColName);
		BaseVectorSummarizer srt = new DenseVectorSummarizer(false);
		for (Row row : in.getOutputTable().getRows()) {
			srt = srt.visit(VectorUtil.getVector(row.getField(idx)));
		}
		return srt.toSummary();
	}

}
