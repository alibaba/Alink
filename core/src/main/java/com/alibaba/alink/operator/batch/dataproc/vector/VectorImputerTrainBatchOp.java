package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.ImputerModelInfo;
import com.alibaba.alink.operator.common.dataproc.vector.VectorImputerModelDataConverter;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.params.dataproc.vector.VectorImputerTrainParams;

/**
 * Imputer completes missing values in a dataSet, but only same type of columns can be selected at the same time.
 * Imputer Train will train a model for predict.
 * Strategy support min, max, mean or value.
 * If min, will replace missing value with min of the column.
 * If max, will replace missing value with max of the column.
 * If mean, will replace missing value with mean of the column.
 * If value, will replace missing value with the input fillValue.
 * Or it will throw "no support" exception.
 */
@InputPorts(values = @PortSpec(value = PortType.DATA))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量缺失值填充训练")
public class VectorImputerTrainBatchOp extends BatchOperator <VectorImputerTrainBatchOp>
	implements VectorImputerTrainParams <VectorImputerTrainBatchOp>,
	WithModelInfoBatchOp <ImputerModelInfo, VectorImputerTrainBatchOp, VectorImputerModelInfoBatchOp> {

	private static final long serialVersionUID = -1427192260071420570L;

	public VectorImputerTrainBatchOp() {
		super(null);
	}

	public VectorImputerTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public VectorImputerTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String vectorColName = getSelectedCol();
		Strategy strategy = getStrategy();

		/* result is statistic model with strategy. */
		VectorImputerModelDataConverter converter = new VectorImputerModelDataConverter();
		converter.vectorColName = vectorColName;

		/* if strategy is not min, max, mean, then only need to write the number. */
		DataSet <Row> rows;
		if (isNeedStatModel()) {
			/* first calculate the data, then transform it into model. */
			rows = StatisticsHelper.vectorSummary(in, vectorColName)
				.flatMap(new BuildVectorImputerModel(vectorColName, strategy));
		} else {
			if (!getParams().contains(VectorImputerTrainParams.FILL_VALUE)) {
				throw new AkIllegalOperatorParameterException("In VALUE strategy, the filling value is necessary.");
			}
			double fillValue = getFillValue();
			RowCollector collector = new RowCollector();
			converter.save(Tuple3.of(Strategy.VALUE, null, fillValue), collector);
			rows = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment().fromCollection(
				collector.getRows());
		}
		this.setOutput(rows, converter.getModelSchema());
		return this;
	}

	@Override
	public VectorImputerModelInfoBatchOp getModelInfoBatchOp() {
		return new VectorImputerModelInfoBatchOp(this.getParams()).linkFrom(this);
	}

	private boolean isNeedStatModel() {
		Strategy strategy = getStrategy();
		if (Strategy.MIN.equals(strategy) || Strategy.MAX.equals(strategy) || Strategy.MEAN.equals(strategy)) {
			return true;
		} else if (Strategy.VALUE.equals(strategy)) {
			return false;
		} else {
			throw new AkIllegalOperatorParameterException("Only support \"MAX\", \"MEAN\", \"MIN\" and \"VALUE\" strategy.");
		}
	}

	/**
	 * table summary build model.
	 */
	public static class BuildVectorImputerModel implements FlatMapFunction <BaseVectorSummary, Row> {
		private static final long serialVersionUID = 4932779293803668991L;
		private String selectedColName;
		private Strategy strategy;

		public BuildVectorImputerModel(String selectedColName, Strategy strategy) {
			this.selectedColName = selectedColName;
			this.strategy = strategy;
		}

		@Override
		public void flatMap(BaseVectorSummary srt, Collector <Row> collector) throws Exception {
			if (null != srt) {
				VectorImputerModelDataConverter converter = new VectorImputerModelDataConverter();
				converter.vectorColName = selectedColName;

				converter.save(new Tuple3 <>(strategy, srt, -1.0), collector);
			}
		}
	}

}
