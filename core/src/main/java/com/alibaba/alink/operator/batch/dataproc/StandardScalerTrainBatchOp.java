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
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.WithModelInfoBatchOp;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.StandardScalerModelDataConverter;
import com.alibaba.alink.operator.common.dataproc.StandardScalerModelInfo;
import com.alibaba.alink.operator.batch.statistics.utils.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.params.dataproc.StandardTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * StandardScaler transforms a dataset, normalizing each feature to have unit standard deviation and/or zero mean.
 */
@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("标准化训练")
@NameEn("Standard Scaler Train")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.dataproc.StandardScaler")
public class StandardScalerTrainBatchOp extends BatchOperator <StandardScalerTrainBatchOp>
	implements StandardTrainParams <StandardScalerTrainBatchOp>,
	WithModelInfoBatchOp <StandardScalerModelInfo, StandardScalerTrainBatchOp, StandardScalerModelInfoBatchOp> {

	private static final long serialVersionUID = 1680956133386337024L;

	public StandardScalerTrainBatchOp() {
		super(null);
	}

	public StandardScalerTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public StandardScalerTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String[] selectedColNames = getSelectedCols();

		TableUtil.assertNumericalCols(in.getSchema(), selectedColNames);

		StandardScalerModelDataConverter converter = new StandardScalerModelDataConverter();
		converter.selectedColNames = selectedColNames;
		converter.selectedColTypes = new TypeInformation[selectedColNames.length];

		for (int i = 0; i < selectedColNames.length; i++) {
			converter.selectedColTypes[i] = Types.DOUBLE;
		}

		DataSet <Row> rows = StatisticsHelper.summary(in, selectedColNames)
			.flatMap(new BuildStandardScalerModel(converter.selectedColNames,
				converter.selectedColTypes,
				getWithMean(),
				getWithStd()));

		this.setOutput(rows, converter.getModelSchema());

		return this;
	}

	/**
	 * table summary build model.
	 */
	public static class BuildStandardScalerModel implements FlatMapFunction <TableSummary, Row> {
		private static final long serialVersionUID = -4019434236075549258L;
		private String[] selectedColNames;
		private TypeInformation[] selectedColTypes;
		private boolean withMean;
		private boolean withStdDevs;

		public BuildStandardScalerModel(String[] selectedColNames, TypeInformation[] selectedColTypes,
										boolean withMean, boolean withStdDevs) {
			this.selectedColNames = selectedColNames;
			this.selectedColTypes = selectedColTypes;
			this.withMean = withMean;
			this.withStdDevs = withStdDevs;
		}

		@Override
		public void flatMap(TableSummary srt, Collector <Row> collector) throws Exception {
			if (null != srt) {
				StandardScalerModelDataConverter converter = new StandardScalerModelDataConverter();
				converter.selectedColNames = selectedColNames;
				converter.selectedColTypes = selectedColTypes;
				converter.save(new Tuple3 <>(this.withMean, this.withStdDevs, srt), collector);
			}
		}
	}

	@Override
	public StandardScalerModelInfoBatchOp getModelInfoBatchOp() {
		return new StandardScalerModelInfoBatchOp(this.getParams()).linkFrom(this);
	}
}
