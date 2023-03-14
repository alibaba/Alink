package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelData;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelDataConverter;
import com.alibaba.alink.operator.common.dataproc.StringIndexerUtil;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.params.dataproc.HasSelectedColTypes;
import com.alibaba.alink.params.feature.CrossFeatureTrainParams;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Cross selected columns to build new vector type data.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.MODEL)})
@ParamSelectColumnSpec(name = "selectedCols")
@NameCn("Cross特征训练")
@NameEn("Cross Feature Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.feature.CrossFeature")
public class CrossFeatureTrainBatchOp extends BatchOperator <CrossFeatureTrainBatchOp>
	implements CrossFeatureTrainParams <CrossFeatureTrainBatchOp> {

	public CrossFeatureTrainBatchOp() {
		super(new Params());
	}

	public CrossFeatureTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public CrossFeatureTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator in = checkAndGetFirst(inputs);
		long mlEnvId = getMLEnvironmentId();
		String[] selectedCols = getSelectedCols();

		final String[] selectedColType = new String[selectedCols.length];
		for (int i = 0; i < selectedCols.length; i++) {
			selectedColType[i] = FlinkTypeConverter.getTypeString(
				TableUtil.findColTypeWithAssertAndHint(in.getSchema(), selectedCols[i]));
		}

		DataSet <Tuple3 <Integer, String, Long>> indexedToken = StringIndexerUtil
			.indexRandom(in.select(selectedCols).getDataSet(), 0L, false);

		DataSet <Row> values = indexedToken
			.mapPartition(new RichMapPartitionFunction <Tuple3 <Integer, String, Long>, Row>() {
				private static final long serialVersionUID = 2876851020570715540L;

				@Override
				public void mapPartition(Iterable <Tuple3 <Integer, String, Long>> values, Collector <Row> out)
					throws Exception {
					Params meta = null;
					if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
						meta = new Params().set(HasSelectedCols.SELECTED_COLS, selectedCols)
							.set(HasSelectedColTypes.SELECTED_COL_TYPES, selectedColType);
					}
					new MultiStringIndexerModelDataConverter().save(Tuple2.of(meta, values), out);
				}
			})
			.name("build_model");


		this.setOutput(values, new MultiStringIndexerModelDataConverter().getModelSchema());

		DataSet <Row> sideDataSet = values
			.mapPartition(new BuildSideOutput())
			.setParallelism(1);

		Table sideModel = DataSetConversionUtil.toTable(mlEnvId, sideDataSet,
			new String[] {"index", "value"},
			new TypeInformation[] {Types.INT, Types.STRING});
		this.setSideOutputTables(new Table[] {sideModel});
		;
		return this;
	}

	private static class BuildSideOutput extends RichMapPartitionFunction <Row, Row> {

		@Override
		public void mapPartition(Iterable <Row> values, Collector <Row> out) throws Exception {
			List <Row> multiStringModelRow = Lists.newArrayList(values);
			MultiStringIndexerModelData modelData = new MultiStringIndexerModelDataConverter()
				.load(multiStringModelRow);
			int featureNumber = modelData.tokenNumber.size();
			String[][] featureToken = new String[featureNumber][];
			int[] tokenSize = new int[featureNumber];
			for (int i = 0; i < featureNumber; i++) {
				int tokenNumber = modelData.tokenNumber.get(i).intValue();
				tokenSize[i] = tokenNumber;
				featureToken[i] = new String[tokenSize[i]];
			}
			for (Tuple3 <Integer, String, Long> tokenAndIndex : modelData.tokenAndIndex) {
				featureToken[tokenAndIndex.f0][tokenAndIndex.f2.intValue()] = tokenAndIndex.f1;
			}

			int[] count = new int[featureNumber + 1];
			count[0] = -1;

			int totalNumber = 1;
			for (int i : tokenSize) {
				totalNumber *= i;
			}

			for (int i = 0; i < totalNumber; i++) {
				//控制进位
				carry(count, tokenSize);
				//拼接
				StringBuilder sbd = new StringBuilder();
				for (int j = 0; j < featureNumber; j++) {
					if (j != 0) {
						sbd.append(", ");
					}
					sbd.append(featureToken[j][count[j]]);
				}
				out.collect(Row.of(i, sbd.toString()));
			}
		}

		private static void carry(int[] count, int[] tokenSize) {
			int start = 0;
			count[start]++;
			while (true) {
				if (count[start] == tokenSize[start]) {
					count[start++] = 0;
					count[start] += 1;
				} else {
					break;
				}
			}
		}
	}
}
