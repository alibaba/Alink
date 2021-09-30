package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.batch.tensorflow.TFTableModelPredictBatchOp;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.operator.common.timeseries.DeepARModelDataConverter;
import com.alibaba.alink.operator.common.timeseries.DeepARModelDataConverter.DeepARModelData;
import com.alibaba.alink.params.timeseries.DeepARBatchPredictParams;

import java.util.ArrayList;
import java.util.List;

public class DeepARPredictBatchOp extends BatchOperator <DeepARPredictBatchOp>
	implements DeepARBatchPredictParams <DeepARPredictBatchOp> {

	@Override
	public DeepARPredictBatchOp linkFrom(BatchOperator <?>... inputs) {
		checkOpSize(2, inputs);

		BatchOperator <?> model = new TableSourceBatchOp(
			DataSetConversionUtil.toTable(
				getMLEnvironmentId(),
				inputs[0]
					.getDataSet()
					.reduceGroup(new GroupReduceFunction <Row, Row>() {
						@Override
						public void reduce(Iterable <Row> values, Collector <Row> out) throws Exception {
							List <Row> all = new ArrayList <>();

							for (Row val : values) {
								all.add(val);
							}

							DeepARModelData modelData = new DeepARModelDataConverter().load(all);

							for (Row val : modelData.deepModel) {
								out.collect(val);
							}
						}
					}),
				CsvUtil.schemaStr2Schema(DeepARModelDataConverter.DEEP_AR_INTERNAL_SCHEMA)
			)
		).setMLEnvironmentId(getMLEnvironmentId());

		BatchOperator <?> metaStr = new TableSourceBatchOp(
			DataSetConversionUtil.toTable(
				getMLEnvironmentId(),
				inputs[0]
					.getDataSet()
					.reduceGroup(new GroupReduceFunction <Row, Row>() {
						@Override
						public void reduce(Iterable <Row> values, Collector <Row> out) throws Exception {
							List <Row> all = new ArrayList <>();

							for (Row val : values) {
								all.add(val);
							}

							DeepARModelData modelData = new DeepARModelDataConverter().load(all);

							out.collect(Row.of(modelData.meta.toJson()));
						}
					}),
				new String[] {"meta"},
				new TypeInformation <?>[] {Types.STRING}
			)
		).setMLEnvironmentId(getMLEnvironmentId());

		BatchOperator <?> processed = new DeepARPreProcessBatchOp(
			getParams()
				.clone()
		).setMLEnvironmentId(getMLEnvironmentId()).linkFrom(inputs[1], metaStr);

		TFTableModelPredictBatchOp predict = new TFTableModelPredictBatchOp()
			.setSelectedCols(new String[] {getSelectedCol()})
			.setSignatureDefKey("serving_default")
			.setInputSignatureDefs(new String[] {"tensor"})
			.setOutputSchemaStr(getPredictionCol() + " TENSOR_TYPES_FLOAT_TENSOR")
			.setOutputSignatureDefs(new String[] {"tf_op_layer_output"})
			.setReservedCols(getReservedCols())
			.setMLEnvironmentId(getMLEnvironmentId())
			.linkFrom(model, processed);

		setOutputTable(predict.getOutputTable());

		return this;
	}
}
