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
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.operator.common.dataproc.ImputerModelInfo;
import com.alibaba.alink.operator.common.dataproc.vector.VectorImputerModelDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.lazy.WithModelInfoLocalOp;
import com.alibaba.alink.params.dataproc.vector.VectorImputerTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

import static com.alibaba.alink.operator.local.dataproc.vector.VectorStandardScalerTrainLocalOp.calcVectorSRT;

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
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.dataproc.vector.VectorImputer")
public class VectorImputerTrainLocalOp extends LocalOperator <VectorImputerTrainLocalOp>
	implements VectorImputerTrainParams <VectorImputerTrainLocalOp>,
	WithModelInfoLocalOp <ImputerModelInfo, VectorImputerTrainLocalOp, VectorImputerModelInfoLocalOp> {

	public VectorImputerTrainLocalOp() {
		super(null);
	}

	public VectorImputerTrainLocalOp(Params params) {
		super(params);
	}

	@Override
	public VectorImputerTrainLocalOp linkFrom(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
		String vectorColName = getSelectedCol();
		Strategy strategy = getStrategy();

		/* result is statistic model with strategy. */
		VectorImputerModelDataConverter converter = new VectorImputerModelDataConverter();
		converter.vectorColName = vectorColName;

		RowCollector rowCollector = new RowCollector();
		if (isNeedStatModel()) {
			/* first calculate the data, then transform it into model. */
			BaseVectorSummary srt = calcVectorSRT(in, vectorColName);
			converter.save(new Tuple3 <>(strategy, srt, -1.0), rowCollector);
		} else {
			/* if strategy is not min, max, mean, then only need to write the number. */
			if (!getParams().contains(VectorImputerTrainParams.FILL_VALUE)) {
				throw new AkIllegalOperatorParameterException("In VALUE strategy, the filling value is necessary.");
			}
			double fillValue = getFillValue();
			converter.save(Tuple3.of(Strategy.VALUE, null, fillValue), rowCollector);
		}

		this.setOutputTable(new MTable(rowCollector.getRows(), converter.getModelSchema()));
		return this;
	}

	@Override
	public VectorImputerModelInfoLocalOp getModelInfoLocalOp() {
		return new VectorImputerModelInfoLocalOp(this.getParams()).linkFrom(this);
	}

	private boolean isNeedStatModel() {
		Strategy strategy = getStrategy();
		if (Strategy.MIN.equals(strategy) || Strategy.MAX.equals(strategy) || Strategy.MEAN.equals(strategy)) {
			return true;
		} else if (Strategy.VALUE.equals(strategy)) {
			return false;
		} else {
			throw new AkIllegalOperatorParameterException(
				"Only support \"MAX\", \"MEAN\", \"MIN\" and \"VALUE\" strategy.");
		}
	}

}
