package com.alibaba.alink.common.dl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.linalg.tensor.TensorTypes;
import com.alibaba.alink.common.linalg.tensor.TensorUtil;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.NumSeqSourceBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.batch.tensorflow.TF2TableModelTrainBatchOp;
import com.alibaba.alink.operator.common.classification.tensorflow.TFTableModelClassificationModelDataConverter;
import com.alibaba.alink.operator.common.regression.tensorflow.TFTableModelRegressionModelDataConverter;
import com.alibaba.alink.operator.common.tensorflow.CommonUtils;
import com.alibaba.alink.operator.common.tensorflow.CommonUtils.ConstructModelFlatMapFunction;
import com.alibaba.alink.operator.common.tensorflow.CommonUtils.CountLabelsMapFunction;
import com.alibaba.alink.params.dl.HasPythonEnv;
import com.alibaba.alink.params.dl.HasTaskType;
import com.alibaba.alink.params.tensorflow.kerasequential.BaseKerasSequentialTrainParams;

import java.util.*;

@Internal
public class BaseKerasSequentialTrainBatchOp<T extends BaseKerasSequentialTrainBatchOp <T>> extends BatchOperator <T>
	implements BaseKerasSequentialTrainParams <T> {

	static final String TF_OUTPUT_SIGNATURE_DEF_CLASSIFICATION = "logits";
	static final String TF_OUTPUT_SIGNATURE_DEF_REGRESSION = "y";
	static final TypeInformation <?> TF_OUTPUT_SIGNATURE_TYPE = TensorTypes.FLOAT_TENSOR;

	private static final String[] RES_PY_FILES = new String[] {
		"res:///tf_algos/train_keras_sequential.py"
	};

	// `mainScriptFileName` and `entryFuncName` are the main file and its entrypoint provided by our wrapping
	private static final String MAIN_SCRIPT_FILE_NAME = "res:///tf_algos/train_keras_sequential.py";

	public BaseKerasSequentialTrainBatchOp() {
		this(null);
	}

	public BaseKerasSequentialTrainBatchOp(Params params) {
		super(params);
	}

	public static String getTfOutputSignatureDef(TaskType taskType) {
		return TaskType.CLASSIFICATION.equals(taskType)
			? TF_OUTPUT_SIGNATURE_DEF_CLASSIFICATION
			: TF_OUTPUT_SIGNATURE_DEF_REGRESSION;
	}

	public BatchOperator <?> toTensorCol(BatchOperator <?> in, String[] tensorColNames) {
		TypeInformation <?>[] origTensorColTypes =
			TableUtil.findColTypesWithAssertAndHint(in.getSchema(), tensorColNames);
		boolean allTensorTypes = Arrays.stream(origTensorColTypes).allMatch(TensorTypes::isTensorType);
		if (allTensorTypes) {
			return in;
		}

		int[] tensorColIndices = TableUtil.findColIndicesWithAssertAndHint(in.getSchema(), tensorColNames);
		DataSet <Row> out = in.getDataSet().map(new MapFunction <Row, Row>() {
			@Override
			public Row map(Row value) throws Exception {
				Row ret = Row.copy(value);
				for (int colIndex : tensorColIndices) {
					ret.setField(colIndex, TensorUtil.getTensor(ret.getField(colIndex)));
				}
				return ret;
			}
		});
		String[] colNames = in.getColNames();
		TypeInformation <?>[] colTypes = in.getColTypes();
		for (int colIndex : tensorColIndices) {
			colTypes[colIndex] = TensorTypes.TENSOR;
		}
		Table table = DataSetConversionUtil.toTable(in.getMLEnvironmentId(), out, colNames, colTypes);
		return new TableSourceBatchOp(table).setMLEnvironmentId(in.getMLEnvironmentId());
	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = inputs[0];
		Params params = getParams();

		TaskType taskType = params.get(HasTaskType.TASK_TYPE);
		boolean isReg = TaskType.REGRESSION.equals(taskType);

		String tensorCol = getTensorCol();
		in = toTensorCol(in, new String[] {tensorCol});

		String labelCol = getLabelCol();
		TypeInformation <?> labelType = TableUtil.findColTypeWithAssertAndHint(in.getSchema(), labelCol);

		DataSet <List <Object>> sortedLabels = null;
		BatchOperator <?> numLabelsOp = null;
		if (!isReg) {
			sortedLabels = in
				.select(labelCol)
				.getDataSet()
				.mapPartition(new MapPartitionFunction<Row, Object>() {
					@Override
					public void mapPartition(Iterable<Row> iterable, Collector<Object> collector) throws Exception {
						Set<Object> distinctValue = new HashSet<>();

						for (Row row : iterable) {
							distinctValue.add(row.getField(0));
						}

						for (Object obj : distinctValue) {
							collector.collect(obj);
						}
					}
				})
				.reduceGroup(new GroupReduceFunction<Object, List<Object>>() {
					@Override
					public void reduce(Iterable<Object> iterable, Collector<List<Object>> collector) throws Exception {
						Set<Object> distinctValue = new TreeSet<>();

						for (Object obj : iterable) {
							distinctValue.add(obj);
						}

						collector.collect(new ArrayList<>(distinctValue));
					}
				});
			in = CommonUtils.mapLabelToIndex(in, labelCol, sortedLabels);
			numLabelsOp = new TableSourceBatchOp(
				DataSetConversionUtil.toTable(
					getMLEnvironmentId(),
					sortedLabels.map(new CountLabelsMapFunction()),
					new String[] {"count"},
					new TypeInformation <?>[] {Types.INT}
				)).setMLEnvironmentId(getMLEnvironmentId());
		}

		Boolean removeCheckpointBeforeTraining = getRemoveCheckpointBeforeTraining();
		if (null == removeCheckpointBeforeTraining) {
			// default to clean checkpoint
			removeCheckpointBeforeTraining = true;
		}

		Map <String, Object> modelConfig = new HashMap <>();
		modelConfig.put("layers", getLayers());

		Map <String, String> userParams = new HashMap <>();
		if (removeCheckpointBeforeTraining) {
			userParams.put(DLConstants.REMOVE_CHECKPOINT_BEFORE_TRAINING, "true");
		}

		userParams.put("tensor_cols", JsonConverter.toJson(new String[] {tensorCol}));
		userParams.put("label_col", labelCol);
		userParams.put("label_type", "float");
		userParams.put("batch_size", String.valueOf(getBatchSize()));
		userParams.put("num_epochs", String.valueOf(getNumEpochs()));
		userParams.put("model_config", JsonConverter.toJson(modelConfig));
		userParams.put("optimizer", getOptimizer());

		if (!StringUtils.isNullOrWhitespaceOnly(getCheckpointFilePath())) {
			userParams.put("model_dir", getCheckpointFilePath());
		}

		ExecutionEnvironment env = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment();
		if (env.getParallelism() == 1) {
			userParams.put("ALINK:ONLY_ONE_WORKER", "true");
		}
		userParams.put("validation_split", String.valueOf(getValidationSplit()));
		userParams.put("save_best_only", String.valueOf(getSaveBestOnly()));
		userParams.put("best_exporter_metric", getBestMetric());
		userParams.put("save_checkpoints_epochs", String.valueOf(getSaveCheckpointsEpochs()));
		if (params.contains(BaseKerasSequentialTrainParams.SAVE_CHECKPOINTS_SECS)) {
			userParams.put("save_checkpoints_secs", String.valueOf(getSaveCheckpointsSecs()));
		}

		TF2TableModelTrainBatchOp trainBatchOp = new TF2TableModelTrainBatchOp(params)
			.setSelectedCols(tensorCol, labelCol)
			.setUserFiles(RES_PY_FILES)
			.setMainScriptFile(MAIN_SCRIPT_FILE_NAME)
			.setUserParams(JsonConverter.toJson(userParams))
			.setIntraOpParallelism(getIntraOpParallelism())
			.setNumPSs(getNumPSs())
			.setNumWorkers(getNumWorkers())
			.setPythonEnv(params.get(HasPythonEnv.PYTHON_ENV));

		if (isReg) {
			trainBatchOp = trainBatchOp.linkFrom(in);
		} else {
			trainBatchOp = trainBatchOp.linkFrom(in, numLabelsOp);
		}

		String tfOutputSignatureDef = getTfOutputSignatureDef(taskType);

		FlatMapOperator <Row, Row> constructModelFlatMapOperator = new NumSeqSourceBatchOp().setFrom(0).setTo(0)
			.setMLEnvironmentId(getMLEnvironmentId())
			.getDataSet()
			.flatMap(new ConstructModelFlatMapFunction(params, new String[] {tensorCol},
				tfOutputSignatureDef, TF_OUTPUT_SIGNATURE_TYPE, null, true))
			.withBroadcastSet(trainBatchOp.getDataSet(), CommonUtils.TF_MODEL_BC_NAME);

		BatchOperator <?> modelOp;
		if (isReg) {
			DataSet <Row> modelDataSet = constructModelFlatMapOperator;
			modelOp = new TableSourceBatchOp(
				DataSetConversionUtil.toTable(
					getMLEnvironmentId(),
					modelDataSet,
					new TFTableModelRegressionModelDataConverter(labelType).getModelSchema()
				)).setMLEnvironmentId(getMLEnvironmentId());
		} else {
			DataSet <Row> modelDataSet = constructModelFlatMapOperator
				.withBroadcastSet(sortedLabels, CommonUtils.SORTED_LABELS_BC_NAME);
			modelOp = new TableSourceBatchOp(
				DataSetConversionUtil.toTable(
					getMLEnvironmentId(),
					modelDataSet,
					new TFTableModelClassificationModelDataConverter(labelType).getModelSchema()
				)).setMLEnvironmentId(getMLEnvironmentId());
		}
		this.setOutputTable(modelOp.getOutputTable());
		return (T) this;
	}
}
