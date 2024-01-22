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
import com.alibaba.alink.operator.common.dataproc.StandardScalerModelDataConverter;
import com.alibaba.alink.operator.common.dataproc.StandardScalerModelInfo;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.lazy.WithModelInfoLocalOp;
import com.alibaba.alink.params.dataproc.StandardTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * StandardScaler transforms a dataset, normalizing each feature to have unit standard deviation and/or zero mean.
 */
@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@ParamSelectColumnSpec(name = "selectCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("标准化训练")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.dataproc.StandardScaler")
public class StandardScalerTrainLocalOp extends LocalOperator <StandardScalerTrainLocalOp>
	implements StandardTrainParams <StandardScalerTrainLocalOp>,
	WithModelInfoLocalOp <StandardScalerModelInfo, StandardScalerTrainLocalOp, StandardScalerModelInfoLocalOp> {

	public StandardScalerTrainLocalOp() {
		super(null);
	}

	public StandardScalerTrainLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
		String[] selectedColNames = getSelectedCols();

		TableUtil.assertNumericalCols(in.getSchema(), selectedColNames);

		StandardScalerModelDataConverter converter = new StandardScalerModelDataConverter();
		converter.selectedColNames = selectedColNames;
		converter.selectedColTypes = new TypeInformation[selectedColNames.length];
		for (int i = 0; i < selectedColNames.length; i++) {
			converter.selectedColTypes[i] = Types.DOUBLE;
		}

		TableSummary srt = in.getOutputTable().summary(selectedColNames);

		RowCollector rowCollector = new RowCollector();
		converter.save(new Tuple3 <>(getWithMean(), getWithStd(), srt), rowCollector);

		this.setOutputTable(new MTable(rowCollector.getRows(), converter.getModelSchema()));
	}

	@Override
	public StandardScalerModelInfoLocalOp getModelInfoLocalOp() {
		return new StandardScalerModelInfoLocalOp(this.getParams()).linkFrom(this);
	}
}
