package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamCond;
import com.alibaba.alink.common.annotation.ParamCond.CondType;
import com.alibaba.alink.common.annotation.ParamMutexRule;
import com.alibaba.alink.common.annotation.ParamMutexRule.ActionType;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.linalg.tensor.TensorUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.tensorflow.TFTableModelTrainBatchOp;
import com.alibaba.alink.operator.common.dataproc.SortUtils;
import com.alibaba.alink.operator.common.tree.Preprocessing;
import com.alibaba.alink.params.timeseries.LSTNetPreProcessParams;
import com.alibaba.alink.params.timeseries.LSTNetTrainParams;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.MODEL))
@ParamSelectColumnSpec(name = "timeCol", allowedTypeCollections = TypeCollections.TIMESTAMP_TYPES)
@ParamSelectColumnSpec(name = "selectedCol")
@ParamSelectColumnSpec(name = "vectorCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@ParamMutexRule(
	name = "vectorCol", type = ActionType.DISABLE,
	cond = @ParamCond(
		name = "selectedCol",
		type = CondType.WHEN_NOT_NULL
	)
)
@ParamMutexRule(
	name = "selectedCol", type = ActionType.DISABLE,
	cond = @ParamCond(
		name = "vectorCol",
		type = CondType.WHEN_NOT_NULL
	)
)
@NameCn("LSTNet训练")
@NameEn("LSTNet Training")
public class LSTNetTrainBatchOp extends BatchOperator <LSTNetTrainBatchOp>
	implements LSTNetTrainParams <LSTNetTrainBatchOp> {

	public LSTNetTrainBatchOp() {
		this(new Params());
	}

	public LSTNetTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public LSTNetTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> input = checkAndGetFirst(inputs);

		BatchOperator <?> preprocessed = new LSTNetPreProcessBatchOp(getParams().clone())
			.setOutputCols("tensor", "y")
			.setMLEnvironmentId(getMLEnvironmentId())
			.linkFrom(input);

		Map <String, Object> modelConfig = new HashMap <>();
		modelConfig.put("window", getWindow());
		modelConfig.put("horizon", getHorizon());

		Map <String, String> userParams = new HashMap <>();
		userParams.put("tensorCol", "tensor");
		userParams.put("labelCol", "y");
		userParams.put("batch_size", String.valueOf(getBatchSize()));
		userParams.put("num_epochs", String.valueOf(getNumEpochs()));
		userParams.put("model_config", JsonConverter.toJson(modelConfig));

		TFTableModelTrainBatchOp tfTableModelTrainBatchOp = new TFTableModelTrainBatchOp(getParams().clone())
			.setSelectedCols("tensor", "y")
			.setUserFiles(new String[] {"res:///tf_algos/lstnet_entry.py"})
			.setMainScriptFile("res:///tf_algos/lstnet_entry.py")
			.setUserParams(JsonConverter.toJson(userParams))
			.linkFrom(preprocessed)
			.setMLEnvironmentId(getMLEnvironmentId());

		setOutputTable(tfTableModelTrainBatchOp.getOutputTable());

		return this;
	}

	@Internal
	private static class LSTNetPreProcessBatchOp extends BatchOperator <LSTNetPreProcessBatchOp>
		implements LSTNetPreProcessParams <LSTNetPreProcessBatchOp> {

		public LSTNetPreProcessBatchOp() {
			this(new Params());
		}

		public LSTNetPreProcessBatchOp(Params params) {
			super(params);
		}

		@Override
		public LSTNetPreProcessBatchOp linkFrom(BatchOperator <?>... inputs) {
			BatchOperator <?> input = checkAndGetFirst(inputs);

			final String colName;

			if (getParams().contains(VECTOR_COL)) {
				colName = getVectorCol();
			} else {
				colName = getSelectedCol();
			}

			AkPreconditions.checkNotNull(colName);

			final String timeCol = getTimeCol();

			input = Preprocessing.select(input, timeCol, colName);

			final int colIndex = TableUtil.findColIndexWithAssertAndHint(input.getColNames(), colName);
			final int timeColIndex = TableUtil.findColIndexWithAssertAndHint(input.getColNames(), timeCol);

			final int window = getWindow();
			final int horizon = getHorizon();

			Tuple2 <DataSet <Tuple2 <Integer, Row>>, DataSet <Tuple2 <Integer, Long>>> sorted =
				SortUtils.pSort(input.getDataSet(), timeColIndex);

			String[] outputColNames = getOutputCols();

			AkPreconditions.checkState(outputColNames != null
				&& (outputColNames.length == 1 || outputColNames.length == 2));

			final boolean genY = outputColNames.length == 2;

			TypeInformation <?>[] outputColTypes = genY ?
				new TypeInformation <?>[] {AlinkTypes.FLOAT_TENSOR, AlinkTypes.FLOAT_TENSOR} :
				new TypeInformation <?>[] {AlinkTypes.FLOAT_TENSOR};

			setOutput(
				sorted.f0
					.partitionByHash(0)
					.mapPartition(
						new MapPartitionFunction <Tuple2 <Integer, Row>, Row>() {
							@Override
							public void mapPartition(Iterable <Tuple2 <Integer, Row>> values,
													 Collector <Row> out) {
								final ArrayList <Tuple2 <Integer, FloatTensor>> tensors = new ArrayList <>();

								for (Tuple2 <Integer, Row> val : values) {
									tensors.add(
										Tuple2.of(
											val.f0,
											FloatTensor.of(TensorUtil.getTensor(val.f1.getField(colIndex)))
										)
									);
								}

								tensors.sort(Comparator.comparing(o -> o.f0));

								// batchify
								int size = tensors.size();

								FloatTensor[] floatTensors = new FloatTensor[window];

								for (int i = window + horizon - 1; i < size; ++i) {
									int end = i - horizon + 1;
									int start = end - window;

									for (int j1 = start, j2 = 0; j1 < end; ++j1, ++j2) {
										floatTensors[j2] = tensors.get(j1).f1;
									}

									if (genY) {
										out.collect(Row.of(Tensor.stack(floatTensors, 0, null), tensors.get(i).f1));
									} else {
										out.collect(Row.of(Tensor.stack(floatTensors, 0, null)));
									}
								}
							}
						}
					),
				outputColNames,
				outputColTypes
			);
			return this;
		}
	}
}
