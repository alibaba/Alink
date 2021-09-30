package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.tensorflow.TFTableModelTrainBatchOp;
import com.alibaba.alink.operator.common.timeseries.DeepARModelDataConverter;
import com.alibaba.alink.operator.common.timeseries.DeepARModelDataConverter.DeepARModelData;
import com.alibaba.alink.params.timeseries.DeepARTrainParams;
import com.alibaba.alink.params.timeseries.HasTimeFrequency;
import com.alibaba.alink.params.timeseries.HasTimeFrequency.TimeFrequency;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeepARTrainBatchOp extends BatchOperator <DeepARTrainBatchOp>
	implements DeepARTrainParams <DeepARTrainBatchOp> {

	@Override
	public DeepARTrainBatchOp linkFrom(BatchOperator <?>... inputs) {

		BatchOperator <?> input = checkAndGetFirst(inputs);

		BatchOperator <?> preprocessed = new DeepARPreProcessBatchOp(getParams().clone())
			.setOutputCols("tensor", "v", "y")
			.setMLEnvironmentId(getMLEnvironmentId())
			.linkFrom(input);

		Map <String, Object> modelConfig = new HashMap <>();
		modelConfig.put("window", getWindow());
		modelConfig.put("stride", getStride());

		Map <String, String> userParams = new HashMap <>();
		userParams.put("tensorCol", "tensor");
		userParams.put("labelCol", "y");
		userParams.put("batch_size", String.valueOf(getBatchSize()));
		userParams.put("num_epochs", String.valueOf(getNumEpochs()));
		userParams.put("model_config", JsonConverter.toJson(modelConfig));

		TFTableModelTrainBatchOp tfTableModelTrainBatchOp = new TFTableModelTrainBatchOp(getParams().clone())
			.setSelectedCols("tensor", "y")
			.setUserFiles(new String[] {"res:///tf_algos/deepar_entry.py"})
			.setMainScriptFile("res:///tf_algos/deepar_entry.py")
			.setUserParams(JsonConverter.toJson(userParams))
			.setMLEnvironmentId(getMLEnvironmentId())
			.linkFrom(preprocessed);

		final Params params = getParams();

		setOutput(
			tfTableModelTrainBatchOp
				.getDataSet()
				.reduceGroup(new RichGroupReduceFunction <Row, Row>() {
					private transient TimeFrequency frequency;

					@Override
					public void open(Configuration parameters) throws Exception {
						frequency = getRuntimeContext()
							.getBroadcastVariableWithInitializer(
								"frequency",
								new BroadcastVariableInitializer <TimeFrequency, TimeFrequency>() {
									@Override
									public TimeFrequency initializeBroadcastVariable(Iterable <TimeFrequency> data) {
										return data.iterator().next();
									}
								}
							);
					}

					@Override
					public void reduce(Iterable <Row> values, Collector <Row> out) throws Exception {
						List <Row> all = new ArrayList <>();

						for (Row val : values) {
							all.add(val);
						}

						new DeepARModelDataConverter().save(
							new DeepARModelData(params.clone().set(HasTimeFrequency.TIME_FREQUENCY, frequency), all),
							out
						);
					}
				})
				.withBroadcastSet(
					preprocessed
						.getSideOutput(0)
						.getDataSet().map(
							new MapFunction <Row, TimeFrequency>() {
								@Override
								public TimeFrequency map(Row value) throws Exception {
									return (TimeFrequency) value.getField(0);
								}
							}),
					"frequency"
				),
			new DeepARModelDataConverter().getModelSchema()
		);

		return this;
	}
}
