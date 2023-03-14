package com.alibaba.alink.operator.local.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
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
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.dataproc.MinMaxScalerModelDataConverter;
import com.alibaba.alink.operator.common.dataproc.MinMaxScalerModelInfo;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.lazy.WithModelInfoLocalOp;
import com.alibaba.alink.params.dataproc.MinMaxScalerTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * MinMaxScaler transforms a dataSet of rows, rescaling each feature
 * to a specific range [min, max). (often [0, 1]).
 * MinMaxScalerTrain will train a model.
 */
@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("归一化训练")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.dataproc.MinMaxScaler")
public class MinMaxScalerTrainLocalOp extends LocalOperator <MinMaxScalerTrainLocalOp>
	implements MinMaxScalerTrainParams <MinMaxScalerTrainLocalOp>,
	WithModelInfoLocalOp <MinMaxScalerModelInfo, MinMaxScalerTrainLocalOp, MinMaxScalerModelInfoLocalOp> {

	public MinMaxScalerTrainLocalOp() {
		super(null);
	}

	public MinMaxScalerTrainLocalOp(Params params) {
		super(params);
	}

	@Override
	public MinMaxScalerTrainLocalOp linkFrom(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
		String[] selectedColNames = getSelectedCols();

		TableUtil.assertNumericalCols(in.getSchema(), selectedColNames);

		//StatisticModel with min and max
		MinMaxScalerModelDataConverter converter = new MinMaxScalerModelDataConverter();
		converter.selectedColNames = selectedColNames;
		converter.selectedColTypes = new TypeInformation[selectedColNames.length];
		for (int i = 0; i < selectedColNames.length; i++) {
			converter.selectedColTypes[i] = Types.DOUBLE;
		}

		TableSummary srt = in.getOutputTable().summary(selectedColNames);

		RowCollector rowCollector = new RowCollector();
		converter.save(new Tuple3 <>(getMin(), getMax(), srt), rowCollector);

		this.setOutputTable(new MTable(rowCollector.getRows(), converter.getModelSchema()));
		return this;
	}

	@Override
	public MinMaxScalerModelInfoLocalOp getModelInfoLocalOp() {
		return new MinMaxScalerModelInfoLocalOp(this.getParams()).linkFrom(this);
	}

}
