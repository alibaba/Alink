package com.alibaba.alink.operator.local.dataproc;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.dataproc.ImputerModelDataConverter;
import com.alibaba.alink.operator.common.dataproc.ImputerModelInfo;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.lazy.WithModelInfoLocalOp;
import com.alibaba.alink.params.dataproc.ImputerTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * Imputer completes missing values in a dataSet, but only same type of columns can be selected at the same time.
 * Imputer Train will train a model for predict.
 * Strategy support min, max, mean or value.
 * If min, will replace missing value with min of the column.
 * If max, will replace missing value with max of the column.
 * If mean, will replace missing value with mean of the column.
 * If value, will replace missing value with the value.
 */
@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@ParamSelectColumnSpec(name = "selectedCols",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("缺失值填充训练")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.dataproc.Imputer")
public class ImputerTrainLocalOp extends LocalOperator <ImputerTrainLocalOp>
	implements ImputerTrainParams <ImputerTrainLocalOp>,
	WithModelInfoLocalOp <ImputerModelInfo, ImputerTrainLocalOp, ImputerModelInfoLocalOp> {

	public ImputerTrainLocalOp() {
		super(null);
	}

	public ImputerTrainLocalOp(Params params) {
		super(params);
	}

	@Override
	public ImputerTrainLocalOp linkFrom(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
		String[] selectedColNames = getSelectedCols();
		Strategy strategy = getStrategy();

		//result is statistic model with strategy.
		ImputerModelDataConverter converter = new ImputerModelDataConverter();
		converter.selectedColNames = selectedColNames;
		converter.selectedColTypes = TableUtil.findColTypesWithAssertAndHint(in.getSchema(), selectedColNames);

		RowCollector rowCollector = new RowCollector();
		if (isNeedStatModel()) {
			TableSummary srt = in.getOutputTable().summary(selectedColNames);
			converter.save(new Tuple3 <>(strategy, srt, ""), rowCollector);
		} else {
			//strategy is not min, max, mean
			if (!getParams().contains(ImputerTrainParams.FILL_VALUE)) {
				throw new AkIllegalOperatorParameterException("In VALUE strategy, the filling value is necessary.");
			}
			String fillValue = getFillValue();
			converter.save(Tuple3.of(Strategy.VALUE, null, fillValue), rowCollector);
		}

		this.setOutputTable(new MTable(rowCollector.getRows(), converter.getModelSchema()));
		return this;
	}

	@Override
	public ImputerModelInfoLocalOp getModelInfoLocalOp() {
		return new ImputerModelInfoLocalOp(this.getParams()).linkFrom(this);
	}

	private boolean isNeedStatModel() {
		Strategy strategy = getStrategy();
		if (Strategy.MIN.equals(strategy) || Strategy.MAX.equals(strategy) || Strategy.MEAN.equals(strategy)) {
			return true;
		} else if (Strategy.VALUE.equals(strategy)) {
			return false;
		} else {
			throw new AkUnsupportedOperationException(
				"Only support \"MAX\", \"MEAN\", \"MIN\" and \"VALUE\" strategy.");
		}
	}

}
