package com.alibaba.alink.operator.local.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
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
import com.alibaba.alink.operator.common.dataproc.MaxAbsScalarModelInfo;
import com.alibaba.alink.operator.common.dataproc.MaxAbsScalerModelDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.lazy.WithModelInfoLocalOp;
import com.alibaba.alink.params.dataproc.MaxAbsScalerTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * MaxAbsScaler transforms a dataSet of rows, rescaling each feature to range
 * [-1, 1] by dividing through the maximum absolute value in each feature.
 * MaxAbsScalerTrain will train a model.
 */
@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("绝对值最大化训练")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.dataproc.MaxAbsScaler")
public class MaxAbsScalerTrainLocalOp extends LocalOperator <MaxAbsScalerTrainLocalOp>
	implements MaxAbsScalerTrainParams <MaxAbsScalerTrainLocalOp>,
	WithModelInfoLocalOp <MaxAbsScalarModelInfo, MaxAbsScalerTrainLocalOp, MaxAbsScalerModelInfoLocalOp> {

	public MaxAbsScalerTrainLocalOp() {
		super(null);
	}

	public MaxAbsScalerTrainLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
		String[] selectedColNames = getSelectedCols();

		TableUtil.assertNumericalCols(in.getSchema(), selectedColNames);

		MaxAbsScalerModelDataConverter converter = new MaxAbsScalerModelDataConverter();
		converter.selectedColNames = selectedColNames;
		converter.selectedColTypes = new TypeInformation[selectedColNames.length];
		for (int i = 0; i < selectedColNames.length; i++) {
			converter.selectedColTypes[i] = Types.DOUBLE;
		}

		TableSummary srt = in.getOutputTable().summary(selectedColNames);

		RowCollector rowCollector = new RowCollector();
		converter.save(srt, rowCollector);

		this.setOutputTable(new MTable(rowCollector.getRows(), converter.getModelSchema()));
	}

	@Override
	public MaxAbsScalerModelInfoLocalOp getModelInfoLocalOp() {
		return new MaxAbsScalerModelInfoLocalOp(this.getParams()).linkFrom(this);
	}

}
