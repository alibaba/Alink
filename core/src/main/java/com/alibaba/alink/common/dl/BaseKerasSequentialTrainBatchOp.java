package com.alibaba.alink.common.dl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.linalg.tensor.TensorTypes;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.NumSeqSourceBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.batch.tensorflow.TF2TableModelTrainBatchOp;
import com.alibaba.alink.operator.common.tensorflow.CommonUtils;
import com.alibaba.alink.operator.common.tensorflow.CommonUtils.ConstructModelFlatMapFunction;
import com.alibaba.alink.operator.common.tensorflow.CommonUtils.CountLabelsMapFunction;
import com.alibaba.alink.operator.common.classification.tensorflow.TFTableModelClassificationModelDataConverter;
import com.alibaba.alink.operator.common.regression.tensorflow.TFTableModelRegressionModelDataConverter;
import com.alibaba.alink.params.dl.HasTaskType;
import com.alibaba.alink.params.tensorflow.kerasequential.BaseKerasSequentialTrainParams;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Internal
public class BaseKerasSequentialTrainBatchOp<T extends BaseKerasSequentialTrainBatchOp <T>> extends BatchOperator <T>
	implements BaseKerasSequentialTrainParams <T> {

	static final String TF_OUTPUT_SIGNATURE_DEF_CLASSIFICATION = "probs";
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

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = inputs[0];
		Params params = getParams();

		TaskType taskType = params.get(HasTaskType.TASK_TYPE);
		boolean isReg = TaskType.REGRESSION.equals(taskType);

		String tensorCol = getTensorCol();
		String labelCol = getLabelCol();

		TypeInformation <?> labelType = TableUtil.findColTypeWithAssertAndHint(in.getSchema(), labelCol);

		DataSet <List <Object>> sortedLabels = null;
		BatchOperator<?> numLabelsOp = null;
		if (!isReg) {
			sortedLabels = in.select(labelCol).distinct().getDataSet()
				.reduceGroup(new CommonUtils.SortLabelsReduceGroupFunction());
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

		TF2TableModelTrainBatchOp trainBatchOp = new TF2TableModelTrainBatchOp(params)
			.setSelectedCols(new String[] {tensorCol, labelCol})
			.setUserFiles(RES_PY_FILES)
			.setMainScriptFile(MAIN_SCRIPT_FILE_NAME)
			.setUserParams(JsonConverter.toJson(userParams))
			.setIntraOpParallelism(getIntraOpParallelism())
			.setNumPSs(getNumPSs())
			.setNumWorkers(getNumWorkers())
			.setPythonEnv(getPythonEnv());

		if (isReg) {
			trainBatchOp = trainBatchOp.linkFrom(in);
		} else {
			trainBatchOp = trainBatchOp.linkFrom(in, numLabelsOp);
		}

		String tfOutputSignatureDef = getTfOutputSignatureDef(taskType);

		FlatMapOperator <Row, Row> constructModelFlatMapOperator = new NumSeqSourceBatchOp().setFrom(0).setTo(0)
			.setMLEnvironmentId(getMLEnvironmentId())
			.getDataSet()
			.flatMap(new ConstructModelFlatMapFunction(params, new String[]{tensorCol}, new String[0],
				tfOutputSignatureDef, TF_OUTPUT_SIGNATURE_TYPE, null))
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
