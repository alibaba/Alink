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
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.vector.VectorMinMaxScalerModelDataConverter;
import com.alibaba.alink.operator.common.dataproc.vector.VectorMinMaxScalerModelInfo;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.params.dataproc.vector.VectorMinMaxScalerTrainParams;

/**
 * MinMaxScaler transforms a dataSet of rows, rescaling each feature
 * to a specific range [min, max). (often [0, 1]).
 * MinMaxScalerTrain will train a model.
 */
@InputPorts(values = @PortSpec(value = PortType.DATA))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量归一化训练")
public final class VectorMinMaxScalerTrainBatchOp extends BatchOperator <VectorMinMaxScalerTrainBatchOp>
	implements VectorMinMaxScalerTrainParams <VectorMinMaxScalerTrainBatchOp>,
	WithModelInfoBatchOp <VectorMinMaxScalerModelInfo, VectorMinMaxScalerTrainBatchOp,
		VectorMinMaxScalerModelInfoBatchOp> {

	private static final long serialVersionUID = 569730809164955534L;

	public VectorMinMaxScalerTrainBatchOp() {
		this(new Params());
	}

	public VectorMinMaxScalerTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public VectorMinMaxScalerTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String vectorColName = getSelectedCol();

		VectorMinMaxScalerModelDataConverter converter = new VectorMinMaxScalerModelDataConverter();
		converter.vectorColName = vectorColName;

		DataSet <Row> rows = StatisticsHelper.vectorSummary(in, vectorColName)
			.flatMap(new BuildVectorMinMaxModel(vectorColName, getMin(), getMax()));

		setOutput(rows, converter.getModelSchema());

		return this;
	}

	@Override
	public VectorMinMaxScalerModelInfoBatchOp getModelInfoBatchOp() {
		return new VectorMinMaxScalerModelInfoBatchOp(this.getParams()).linkFrom(this);
	}

	/**
	 * table summary build model.
	 */
	public static class BuildVectorMinMaxModel implements FlatMapFunction <BaseVectorSummary, Row> {
		private static final long serialVersionUID = -824127373968536758L;
		private String selectedColName;
		private double min;
		private double max;

		public BuildVectorMinMaxModel(String selectedColName, double min, double max) {
			this.selectedColName = selectedColName;
			this.min = min;
			this.max = max;
		}

		@Override
		public void flatMap(BaseVectorSummary srt, Collector <Row> collector) throws Exception {
			if (null != srt) {
				VectorMinMaxScalerModelDataConverter converter = new VectorMinMaxScalerModelDataConverter();
				converter.vectorColName = selectedColName;

				converter.save(Tuple3.of(min, max, srt), collector);
			}
		}
	}

}
