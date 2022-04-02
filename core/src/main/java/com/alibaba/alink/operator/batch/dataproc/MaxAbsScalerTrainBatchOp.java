package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
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
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.MaxAbsScalarModelInfo;
import com.alibaba.alink.operator.common.dataproc.MaxAbsScalerModelDataConverter;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.params.dataproc.MaxAbsScalerTrainParams;

/**
 * MaxAbsScaler transforms a dataSet of rows, rescaling each feature to range
 * [-1, 1] by dividing through the maximum absolute value in each feature.
 * MaxAbsScalerTrain will train a model.
 */
@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("绝对值最大化训练")
public class MaxAbsScalerTrainBatchOp extends BatchOperator <MaxAbsScalerTrainBatchOp>
	implements MaxAbsScalerTrainParams <MaxAbsScalerTrainBatchOp>,
	WithModelInfoBatchOp <MaxAbsScalarModelInfo, MaxAbsScalerTrainBatchOp, MaxAbsScalerModelInfoBatchOp> {

	private static final long serialVersionUID = -5277380717846768030L;

	public MaxAbsScalerTrainBatchOp() {
		super(null);
	}

	public MaxAbsScalerTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public MaxAbsScalerTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String[] selectedColNames = getSelectedCols();

		TableUtil.assertNumericalCols(in.getSchema(), selectedColNames);

		MaxAbsScalerModelDataConverter converter = new MaxAbsScalerModelDataConverter();
		converter.selectedColNames = selectedColNames;
		converter.selectedColTypes = new TypeInformation[selectedColNames.length];

		for (int i = 0; i < selectedColNames.length; i++) {
			converter.selectedColTypes[i] = Types.DOUBLE;
		}

		DataSet <Row> rows = StatisticsHelper.summary(in, selectedColNames)
			.flatMap(new BuildMaxAbsScalerModel(converter.selectedColNames, converter.selectedColTypes));

		this.setOutput(rows, converter.getModelSchema());

		return this;
	}

	@Override
	public MaxAbsScalerModelInfoBatchOp getModelInfoBatchOp() {
		return new MaxAbsScalerModelInfoBatchOp(this.getParams()).linkFrom(this);
	}

	/**
	 * table summary build model.
	 */
	public static class BuildMaxAbsScalerModel implements FlatMapFunction <TableSummary, Row> {
		private static final long serialVersionUID = -7623894959532274310L;
		private String[] selectedColNames;
		private TypeInformation[] selectedColTypes;

		public BuildMaxAbsScalerModel(String[] selectedColNames, TypeInformation[] selectedColTypes) {
			this.selectedColNames = selectedColNames;
			this.selectedColTypes = selectedColTypes;
		}

		@Override
		public void flatMap(TableSummary srt, Collector <Row> collector) throws Exception {
			if (null != srt) {
				MaxAbsScalerModelDataConverter converter = new MaxAbsScalerModelDataConverter();
				converter.selectedColNames = selectedColNames;
				converter.selectedColTypes = selectedColTypes;
				converter.save(srt, collector);
			}
		}
	}
}
