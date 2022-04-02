package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.vector.VectorStandardScalerModelDataConverter;
import com.alibaba.alink.operator.common.dataproc.vector.VectorStandardScalerModelInfo;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.params.dataproc.vector.VectorStandardTrainParams;

/**
 * StandardScaler transforms a dataSet, normalizing each feature to have unit standard deviation and/or zero mean.
 * If withMean is false, set mean as 0; if withStd is false, set std as 1.
 */
@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量标准化训练")
public final class VectorStandardScalerTrainBatchOp extends BatchOperator <VectorStandardScalerTrainBatchOp>
	implements VectorStandardTrainParams <VectorStandardScalerTrainBatchOp>,
	WithModelInfoBatchOp <VectorStandardScalerModelInfo, VectorStandardScalerTrainBatchOp,
		VectorStandardScalerModelInfoBatchOp> {

	private static final long serialVersionUID = 6287488179034845512L;

	public VectorStandardScalerTrainBatchOp() {
		this(new Params());
	}

	public VectorStandardScalerTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public VectorStandardScalerTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String vectorColName = getSelectedCol();

		VectorStandardScalerModelDataConverter converter = new VectorStandardScalerModelDataConverter();
		converter.vectorColName = vectorColName;

		DataSet <Row> rows = StatisticsHelper.vectorSummary(in, vectorColName)
			.flatMap(new BuildVectorStandardModel(vectorColName, getWithMean(), getWithStd()));

		setOutput(rows, converter.getModelSchema());

		return this;
	}

	@Override
	public VectorStandardScalerModelInfoBatchOp getModelInfoBatchOp() {
		return new VectorStandardScalerModelInfoBatchOp(this.getParams()).linkFrom(this);
	}

	/**
	 * table summary build model.
	 */
	public static class BuildVectorStandardModel implements FlatMapFunction <BaseVectorSummary, Row> {
		private static final long serialVersionUID = 4685384519703499258L;
		private String selectedColName;
		private boolean withMean;
		private boolean withStd;

		public BuildVectorStandardModel(String selectedColName, boolean withMean, boolean withStd) {
			this.selectedColName = selectedColName;
			this.withMean = withMean;
			this.withStd = withStd;
		}

		@Override
		public void flatMap(BaseVectorSummary srt, Collector <Row> collector) throws Exception {
			if (null != srt) {
				VectorStandardScalerModelDataConverter converter = new VectorStandardScalerModelDataConverter();
				converter.vectorColName = selectedColName;
				converter.save(Tuple3.of(withMean, withStd, srt), collector);
			}
		}
	}
}
