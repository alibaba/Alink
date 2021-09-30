package com.alibaba.alink.common.dl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.NumSeqSourceBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.nlp.bert.BertTokenizerMapper;
import com.alibaba.alink.operator.common.nlp.bert.tokenizer.EncodingKeys;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.operator.common.tensorflow.CommonUtils.ConstructModelFlatMapFunction;
import com.alibaba.alink.operator.common.tensorflow.CommonUtils.SortLabelsReduceGroupFunction;
import com.alibaba.alink.operator.common.classification.tensorflow.TFTableModelClassificationModelDataConverter;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.params.dl.HasModelPath;
import com.alibaba.alink.params.dl.HasTaskType;
import com.alibaba.alink.params.dl.HasUserFiles;
import com.alibaba.alink.params.tensorflow.bert.BaseEasyTransferTrainParams;
import com.alibaba.alink.params.tensorflow.bert.HasBertModelName;
import com.alibaba.alink.params.tensorflow.bert.HasCustomConfigJson;
import com.alibaba.alink.params.tensorflow.bert.HasTaskName;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.nlp.BertTokenizer;
import com.google.common.collect.ImmutableMap;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.alink.common.dl.EasyTransferUtils.TF_OUTPUT_SIGNATURE_TYPE;
import static com.alibaba.alink.common.dl.EasyTransferUtils.mapLabelToIntIndex;
import static com.alibaba.alink.operator.common.tensorflow.CommonUtils.PREPROCESS_PIPELINE_MODEL_BC_NAME;
import static com.alibaba.alink.operator.common.tensorflow.CommonUtils.SORTED_LABELS_BC_NAME;
import static com.alibaba.alink.operator.common.tensorflow.CommonUtils.TF_MODEL_BC_NAME;

@Internal
public class BaseEasyTransferTrainBatchOp<T extends BaseEasyTransferTrainBatchOp <T>> extends BatchOperator <T>
	implements BaseEasyTransferTrainParams <T>, HasTaskName <T>, HasTaskType <T> {

	private static final String[] MODEL_INPUTS = new String[] {
		EncodingKeys.INPUT_IDS_KEY.label,
		EncodingKeys.TOKEN_TYPE_IDS_KEY.label,
		EncodingKeys.ATTENTION_MASK_KEY.label
	};

	private static final String[] SAFE_MODEL_INPUTS = Arrays.stream(MODEL_INPUTS)
		.map(BertTokenizerMapper::prependPrefix)
		.toArray(String[]::new);

	public BaseEasyTransferTrainBatchOp() {
		this(new Params());
	}

	public BaseEasyTransferTrainBatchOp(Params params) {
		super(params);
	}

	public static Map <String, Object> getPreprocessConfig(Params params, boolean doTokenizer) {
		Map <String, Object> config = new HashMap <>();
		String textCol = params.get(BaseEasyTransferTrainParams.TEXT_COL);
		String textPairCol = params.get(BaseEasyTransferTrainParams.TEXT_PAIR_COL);
		String labelCol = params.get(BaseEasyTransferTrainParams.LABEL_COL);
		int maxSeqLength = params.get(BaseEasyTransferTrainParams.MAX_SEQ_LENGTH);
		TaskType taskType = params.get(HasTaskType.TASK_TYPE);
		String labelTypeStr = TaskType.CLASSIFICATION.equals(taskType) ? "int" : "float";

		String inputSchema = doTokenizer
			? (null == textPairCol
			? String.format("%s:float:1,%s:str:1", labelCol, textCol)
			: String.format("%s:float:1,%s:str:1,%s:str:1", labelCol, textCol, textPairCol))
			: String.format("%s:int:%d,%s:int:%d,%s:int:%d,%s:%s:1",
				SAFE_MODEL_INPUTS[0], maxSeqLength,
				SAFE_MODEL_INPUTS[1], maxSeqLength,
				SAFE_MODEL_INPUTS[2], maxSeqLength,
				labelCol, labelTypeStr);

		config.put("input_schema", inputSchema);
		if (doTokenizer) {
			config.put("first_sequence", textCol);
			if (null != textPairCol) {
				config.put("second_sequence", textPairCol);
			}
		}
		config.put("sequence_length", maxSeqLength);
		config.put("label_name", labelCol);
		if (TaskType.CLASSIFICATION.equals(taskType)) {
			config.put("num_labels", 2);
			config.put("label_enumerate_values", "0.0,1.0");    // TODO: check
		} else {
			config.put("num_labels", 1);
		}
		return config;
	}

	public static Map <String, Object> getModelConfig(Params params) {
		Map <String, Object> config = new HashMap <>();
		if (!params.contains(BaseEasyTransferTrainParams.MODEL_PATH) || (null == params.get(
			BaseEasyTransferTrainParams.MODEL_PATH))) {
			params.set(BaseEasyTransferTrainParams.MODEL_PATH,
				BertResources.getBertModelCkpt(params.get(BaseEasyTransferTrainParams.BERT_MODEL_NAME)));
		}

		TaskType taskType = params.get(HasTaskType.TASK_TYPE);
		if (TaskType.CLASSIFICATION.equals(taskType)) {
			config.put("num_labels", 2);
		} else {
			config.put("num_labels", 1);
		}
		int numFineTunedLayers = params.get(BaseEasyTransferTrainParams.NUM_FINE_TUNED_LAYERS);
		config.put("dropout_rate", 0.3);    // TODO
		config.put("num_freezed_layers", Math.max(0, 12 - numFineTunedLayers));	// TODO: check 12
		config.put("keep_checkpoint_max", 1);
		return config;
	}

	public static Map <String, Object> getTrainConfig(Params params) {
		Map <String, Object> config = new HashMap <>();
		int batchSize = params.get(BaseEasyTransferTrainParams.BATCH_SIZE);
		double numEpochs = params.get(BaseEasyTransferTrainParams.NUM_EPOCHS);
		double learningRate = params.get(BaseEasyTransferTrainParams.LEARNING_RATE);
		config.put("train_batch_size", batchSize);
		config.put("save_steps", 10);
		config.put("num_epochs", numEpochs);
		config.put("optimizer_config", ImmutableMap.of("learning_rate", learningRate));
		return config;
	}

	public static Map <String, Object> getExportConfig(Params params) {
		String labelCol = params.get(BaseEasyTransferTrainParams.LABEL_COL);
		int maxSeqLength = params.get(BaseEasyTransferTrainParams.MAX_SEQ_LENGTH);
		TaskType taskType = params.get(HasTaskType.TASK_TYPE);
		String labelTypeStr = TaskType.CLASSIFICATION.equals(taskType) ? "int" : "float";
		Map <String, Object> config = new HashMap <>();
		config.put("input_tensors_schema",
			String.format("%s:int:%d,%s:int:%d,%s:int:%d,%s:%s:1",
				SAFE_MODEL_INPUTS[0], maxSeqLength,
				SAFE_MODEL_INPUTS[1], maxSeqLength,
				SAFE_MODEL_INPUTS[2], maxSeqLength,
				labelCol, labelTypeStr));
		config.put("receiver_tensors_schema",
			String.format("%s:int:%d,%s:int:%d,%s:int:%d",
				SAFE_MODEL_INPUTS[0], maxSeqLength,
				SAFE_MODEL_INPUTS[1], maxSeqLength,
				SAFE_MODEL_INPUTS[2], maxSeqLength));
		return config;
	}

	/**
	 * Merge two maps with String key type. If a value in the map is a map, then it must has String key type.
	 * <p>
	 * When two maps contains a same key, if both corresponding values are maps, then recursive merging is applied.
	 * Otherwise, the corresponding value in `newM` is accepted.
	 *
	 * @param baseM
	 * @param newM
	 * @return
	 */
	public static Map <String, Object> mergeMap(Map <String, Object> baseM, Map <String, Object> newM) {
		if (null == newM) {
			return new HashMap <>(baseM);
		}
		Set <String> keys = new HashSet <>(baseM.keySet());
		keys.addAll(newM.keySet());
		Map <String, Object> m = new HashMap <>();
		for (String key : keys) {
			Object baseObj = baseM.get(key);
			Object newObj = newM.get(key);
			if (null == baseObj) {
				m.put(key, newObj);
			} else if (null == newObj) {
				m.put(key, baseObj);
			} else if (baseObj instanceof Map && newObj instanceof Map) {
				//noinspection unchecked
				m.put(key, mergeMap((Map <String, Object>) baseObj, (Map <String, Object>) newObj));
			} else {
				m.put(key, newObj);
			}
		}
		return m;
	}

	public static Map <String, Map <String, Object>> getConfig(Params params, boolean doTokenizer) {
		Map <String, Map <String, Object>> customConfig;
		if (params.contains(HasCustomConfigJson.CUSTOM_CONFIG_JSON)) {
			String customConfigJson = params.get(HasCustomConfigJson.CUSTOM_CONFIG_JSON);
			customConfig = JsonConverter.fromJson(customConfigJson,
				new TypeToken <Map <String, Map <String, Object>>>() {}.getType());
		} else {
			customConfig = new HashMap <>();
		}

		Map <String, Map <String, Object>> config = new HashMap <>();
		config.put("preprocess_config", mergeMap(getPreprocessConfig(params, doTokenizer), customConfig.get("preprocess_config")));
		config.put("model_config", mergeMap(getModelConfig(params), customConfig.get("model_config")));
		config.put("train_config", mergeMap(getTrainConfig(params), customConfig.get("train_config")));
		config.put("export_config", mergeMap(getExportConfig(params), customConfig.get("export_config")));
		return config;
	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = inputs[0];
		Params params = getParams();
		TaskType taskType = params.get(HasTaskType.TASK_TYPE);
		String labelCol = getLabelCol();
		TypeInformation <?> labelType = TableUtil.findColTypeWithAssertAndHint(in.getSchema(), labelCol);

		if (null != getSelectedCols()) {
			in = in.select(getSelectedCols());
		}

		DataSet <List <Object>> sortedLabels = null;
		if (TaskType.CLASSIFICATION.equals(taskType)) {
			sortedLabels = in.select(labelCol).distinct().getDataSet()
				.reduceGroup(new SortLabelsReduceGroupFunction());
			in = mapLabelToIntIndex(in, labelCol, sortedLabels);
		}

		BertTokenizer bertTokenizer = new BertTokenizer(params.clone());
		PipelineModel preprocessPipelineMode = new PipelineModel(bertTokenizer);
		in = preprocessPipelineMode.transform(in);
		BatchOperator <?> preprocessPipelineModelOp = preprocessPipelineMode.save();
		String preprocessPipelineModelSchemaStr = CsvUtil.schema2SchemaStr(preprocessPipelineModelOp.getSchema());

		Map <String, String> userParams = new HashMap <>();

		String bertModelName = params.get(HasBertModelName.BERT_MODEL_NAME);
		String bertModelCkptPath =
			params.contains(HasModelPath.MODEL_PATH) && (null != params.get(HasModelPath.MODEL_PATH))
			? params.get(HasModelPath.MODEL_PATH)
			: BertResources.getBertModelCkpt(bertModelName);

		if (!StringUtils.isNullOrWhitespaceOnly(getCheckpointFilePath())) {
			userParams.put("model_dir", getCheckpointFilePath());
		}

		ExternalFilesConfig externalFilesConfig = params.contains(HasUserFiles.USER_FILES)
			? ExternalFilesConfig.fromJson(params.get(HasUserFiles.USER_FILES))
			: new ExternalFilesConfig();
		externalFilesConfig.addFilePaths(bertModelCkptPath);
		userParams.put("pretrained_ckpt_path", PythonFileUtils.getCompressedFileName(bertModelCkptPath));

		Map <String, Map <String, Object>> config = getConfig(getParams(), false);
		String configJson = JsonConverter.toJson(config);
		System.out.println("config : " + configJson);
		userParams.put("app_name", getTaskName().name());

		EasyTransferConfigTrainBatchOp trainBatchOp = new EasyTransferConfigTrainBatchOp()
			.setSelectedCols(ArrayUtils.add(SAFE_MODEL_INPUTS, labelCol))
			.setConfigJson(configJson)
			.setUserFiles(externalFilesConfig)
			.setNumWorkers(getNumWorkers())
			.setNumPSs(getNumPSs())
			.setUserParams(JsonConverter.toJson(userParams))
			.setPythonEnv(getPythonEnv())
			.setIntraOpParallelism(getIntraOpParallelism())
			.setMLEnvironmentId(getMLEnvironmentId());

		BatchOperator <?>[] tfInputs;
		tfInputs = new BatchOperator <?>[inputs.length];
		tfInputs[0] = in;
		System.arraycopy(inputs, 1, tfInputs, 1, inputs.length - 1);
		BatchOperator <?> tfModel = trainBatchOp.linkFrom(tfInputs);

		String tfOutputSignatureDef = EasyTransferUtils.getTfOutputSignatureDef(taskType);

		FlatMapOperator <Row, Row> constructModelFlatMapOperator = new NumSeqSourceBatchOp().setFrom(0).setTo(0)
			.setMLEnvironmentId(getMLEnvironmentId())
			.getDataSet()
			.flatMap(new ConstructModelFlatMapFunction(params, SAFE_MODEL_INPUTS, new String[0],
				tfOutputSignatureDef, TF_OUTPUT_SIGNATURE_TYPE, preprocessPipelineModelSchemaStr))
			.withBroadcastSet(preprocessPipelineModelOp.getDataSet(), PREPROCESS_PIPELINE_MODEL_BC_NAME)
			.withBroadcastSet(tfModel.getDataSet(), TF_MODEL_BC_NAME);

		DataSet <Row> modelDataSet = TaskType.CLASSIFICATION.equals(taskType)
			? constructModelFlatMapOperator.withBroadcastSet(sortedLabels, SORTED_LABELS_BC_NAME)
			: constructModelFlatMapOperator;

		BatchOperator <?> modelOp = new TableSourceBatchOp(
			DataSetConversionUtil.toTable(
				getMLEnvironmentId(),
				modelDataSet,
				new TFTableModelClassificationModelDataConverter(labelType).getModelSchema()
			)).setMLEnvironmentId(getMLEnvironmentId());

		this.setOutputTable(modelOp.getOutputTable());
		return (T) this;
	}
}
