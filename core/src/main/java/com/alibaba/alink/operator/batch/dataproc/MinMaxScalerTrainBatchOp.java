package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
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
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.MinMaxScalerModelDataConverter;
import com.alibaba.alink.operator.common.dataproc.MinMaxScalerModelInfo;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.params.dataproc.MinMaxScalerTrainParams;

/**
 * MinMaxScaler transforms a dataSet of rows, rescaling each feature
 * to a specific range [min, max). (often [0, 1]).
 * MinMaxScalerTrain will train a model.
 */
@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("归一化训练")
public class MinMaxScalerTrainBatchOp extends BatchOperator <MinMaxScalerTrainBatchOp>
	implements MinMaxScalerTrainParams <MinMaxScalerTrainBatchOp>,
	WithModelInfoBatchOp <MinMaxScalerModelInfo, MinMaxScalerTrainBatchOp, MinMaxScalerModelInfoBatchOp> {

	private static final long serialVersionUID = 680992643768892723L;

	public MinMaxScalerTrainBatchOp() {
		super(null);
	}

	public MinMaxScalerTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public MinMaxScalerTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String[] selectedColNames = getSelectedCols();

		TableUtil.assertNumericalCols(in.getSchema(), selectedColNames);

		//StatisticModel with min and max
		MinMaxScalerModelDataConverter converter = new MinMaxScalerModelDataConverter();
		converter.selectedColNames = selectedColNames;
		converter.selectedColTypes = new TypeInformation[selectedColNames.length];

		for (int i = 0; i < selectedColNames.length; i++) {
			converter.selectedColTypes[i] = Types.DOUBLE;
		}

		DataSet <Row> rows = StatisticsHelper.summary(in, selectedColNames)
			.flatMap(new BuildMinMaxScalerModel(
				converter.selectedColNames,
				converter.selectedColTypes,
				getMin(), getMax()));

		this.setOutput(rows, converter.getModelSchema());
		return this;
	}

	@Override
	public MinMaxScalerModelInfoBatchOp getModelInfoBatchOp() {
		return new MinMaxScalerModelInfoBatchOp(this.getParams()).linkFrom(this);
	}

	/**
	 * table summary build model.
	 */
	public static class BuildMinMaxScalerModel implements FlatMapFunction <TableSummary, Row> {
		private static final long serialVersionUID = 7545982206669132954L;
		private String[] selectedColNames;
		private TypeInformation[] selectedColTypes;
		private double min;
		private double max;

		public BuildMinMaxScalerModel(String[] selectedColNames, TypeInformation[] selectedColTypes,
									  double min, double max) {
			this.selectedColNames = selectedColNames;
			this.selectedColTypes = selectedColTypes;
			this.min = min;
			this.max = max;
		}

		@Override
		public void flatMap(TableSummary srt, Collector <Row> collector) throws Exception {
			if (null != srt) {
				MinMaxScalerModelDataConverter converter = new MinMaxScalerModelDataConverter();
				converter.selectedColNames = selectedColNames;
				converter.selectedColTypes = selectedColTypes;

				converter.save(new Tuple3 <>(min, max, srt), collector);
			}
		}
	}

}
