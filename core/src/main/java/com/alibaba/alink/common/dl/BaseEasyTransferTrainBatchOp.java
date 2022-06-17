package com.alibaba.alink.common.dl;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.io.plugin.ResourcePluginFactory;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.classification.tensorflow.TFTableModelClassificationModelDataConverter;
import com.alibaba.alink.operator.common.nlp.bert.BertTokenizerMapper;
import com.alibaba.alink.operator.common.nlp.bert.tokenizer.EncodingKeys;
import com.alibaba.alink.operator.common.tensorflow.CommonUtils.ConstructModelMapPartitionFunction;
import com.alibaba.alink.operator.common.tensorflow.CommonUtils.SortLabelsReduceGroupFunction;
import com.alibaba.alink.params.dl.HasBatchSizeDefaultAs32;
import com.alibaba.alink.params.dl.HasCheckpointFilePathDefaultAsNull;
import com.alibaba.alink.params.dl.HasIntraOpParallelism;
import com.alibaba.alink.params.dl.HasLearningRateDefaultAs0001;
import com.alibaba.alink.params.dl.HasModelPath;
import com.alibaba.alink.params.dl.HasNumPssDefaultAsNull;
import com.alibaba.alink.params.dl.HasNumWorkersDefaultAsNull;
import com.alibaba.alink.params.dl.HasPythonEnv;
import com.alibaba.alink.params.dl.HasTaskType;
import com.alibaba.alink.params.dl.HasUserFiles;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.tensorflow.bert.HasBertModelName;
import com.alibaba.alink.params.tensorflow.bert.HasCustomConfigJson;
import com.alibaba.alink.params.tensorflow.bert.HasMaxSeqLength;
import com.alibaba.alink.params.tensorflow.bert.HasMaxSeqLengthDefaultAsNull;
import com.alibaba.alink.params.tensorflow.bert.HasNumEpochsDefaultAs001;
import com.alibaba.alink.params.tensorflow.bert.HasNumFineTunedLayersDefaultAs1;
import com.alibaba.alink.params.tensorflow.bert.HasTaskName;
import com.alibaba.alink.params.tensorflow.bert.HasTextCol;
import com.alibaba.alink.params.tensorflow.bert.HasTextPairCol;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.nlp.BertTokenizer;
import com.google.common.collect.ImmutableMap;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.MODEL))
@Internal
public class BaseEasyTransferTrainBatchOp<T extends BaseEasyTransferTrainBatchOp <T>> extends BatchOperator <T> {

	private static final Logger LOG = LoggerFactory.getLogger(BaseEasyTransferTrainBatchOp.class);

	private static final String[] MODEL_INPUTS = new String[] {
		EncodingKeys.INPUT_IDS_KEY.label,
		EncodingKeys.TOKEN_TYPE_IDS_KEY.label,
		EncodingKeys.ATTENTION_MASK_KEY.label
	};

	private static final String[] SAFE_MODEL_INPUTS = Arrays.stream(MODEL_INPUTS)
		.map(BertTokenizerMapper::prependPrefix)
		.toArray(String[]::new);

	private final ResourcePluginFactory factory;

	public BaseEasyTransferTrainBatchOp() {
		this(new Params());
	}

	public BaseEasyTransferTrainBatchOp(Params params) {
		super(params);
		factory = new ResourcePluginFactory();
	}

	public static Map <String, Object> getPreprocessConfig(Params params, boolean doTokenizer) {
		Map <String, Object> config = new HashMap <>();
		String textCol = params.get(HasTextCol.TEXT_COL);
		String textPairCol = params.contains(HasTextPairCol.TEXT_PAIR_COL)
			? params.get(HasTextPairCol.TEXT_PAIR_COL)
			: null;
		String labelCol = params.get(HasLabelCol.LABEL_COL);
		int maxSeqLength = params.get(HasMaxSeqLength.MAX_SEQ_LENGTH);
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

	public static Map <String, Object> getModelConfig(Params params, ResourcePluginFactory factory) {
		Map <String, Object> config = new HashMap <>();
		if (!params.contains(HasModelPath.MODEL_PATH) || (null == params.get(HasModelPath.MODEL_PATH))) {
			params.set(HasModelPath.MODEL_PATH,
				BertResources.getBertModelCkpt(factory, params.get(HasBertModelName.BERT_MODEL_NAME)));
		}

		TaskType taskType = params.get(HasTaskType.TASK_TYPE);
		if (TaskType.CLASSIFICATION.equals(taskType)) {
			config.put("num_labels", 2);
		} else {
			config.put("num_labels", 1);
		}
		int numFineTunedLayers = params.get(HasNumFineTunedLayersDefaultAs1.NUM_FINE_TUNED_LAYERS);
		config.put("dropout_rate", 0.3);    // TODO
		config.put("num_freezed_layers", Math.max(0, 12 - numFineTunedLayers));
		config.put("keep_checkpoint_max", 1);
		return config;
	}

	public static Map <String, Object> getTrainConfig(Params params) {
		Map <String, Object> config = new HashMap <>();
		int batchSize = params.get(HasBatchSizeDefaultAs32.BATCH_SIZE);
		double numEpochs = params.get(HasNumEpochsDefaultAs001.NUM_EPOCHS);
		double learningRate = params.get(HasLearningRateDefaultAs0001.LEARNING_RATE);
		config.put("train_batch_size", batchSize);
		config.put("save_steps", 100);
		config.put("num_epochs", numEpochs);
		config.put("optimizer_config", ImmutableMap.of("learning_rate", learningRate));
		return config;
	}

	public static Map <String, Object> getExportConfig(Params params) {
		String labelCol = params.get(HasLabelCol.LABEL_COL);
		int maxSeqLength = params.get(HasMaxSeqLength.MAX_SEQ_LENGTH);
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

	public static Map <String, Map <String, Object>> getConfig(Params params, boolean doTokenizer,
															   ResourcePluginFactory factory) {
		Map <String, Map <String, Object>> customConfig;
		if (params.contains(HasCustomConfigJson.CUSTOM_CONFIG_JSON)) {
			String customConfigJson = params.get(HasCustomConfigJson.CUSTOM_CONFIG_JSON);
			customConfig = JsonConverter.fromJson(customConfigJson,
				new TypeToken <Map <String, Map <String, Object>>>() {}.getType());
		} else {
			customConfig = new HashMap <>();
		}

		Map <String, Map <String, Object>> config = new HashMap <>();
		config.put("preprocess_config",
			mergeMap(getPreprocessConfig(params, doTokenizer), customConfig.get("preprocess_config")));
		config.put("model_config", mergeMap(getModelConfig(params, factory), customConfig.get("model_config")));
		config.put("train_config", mergeMap(getTrainConfig(params), customConfig.get("train_config")));
		config.put("export_config", mergeMap(getExportConfig(params), customConfig.get("export_config")));
		return config;
	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = inputs[0];
		Params params = getParams();
		TaskType taskType = params.get(HasTaskType.TASK_TYPE);
		String labelCol = params.get(HasLabelCol.LABEL_COL);
		TypeInformation <?> labelType = TableUtil.findColTypeWithAssertAndHint(in.getSchema(), labelCol);

		DataSet <List <Object>> sortedLabels = null;
		if (TaskType.CLASSIFICATION.equals(taskType)) {
			sortedLabels = in.select(labelCol).distinct().getDataSet()
				.reduceGroup(new SortLabelsReduceGroupFunction());
			in = mapLabelToIntIndex(in, labelCol, sortedLabels);
		}

		BertTokenizer bertTokenizer = new BertTokenizer(params.clone())
			.set(HasMaxSeqLengthDefaultAsNull.MAX_SEQ_LENGTH, params.get(HasMaxSeqLength.MAX_SEQ_LENGTH));
		PipelineModel preprocessPipelineMode = new PipelineModel(bertTokenizer);
		in = preprocessPipelineMode.transform(in);
		BatchOperator <?> preprocessPipelineModelOp = preprocessPipelineMode.save();
		String preprocessPipelineModelSchemaStr = TableUtil.schema2SchemaStr(preprocessPipelineModelOp.getSchema());

		Map <String, String> userParams = new HashMap <>();

		String bertModelName = params.get(HasBertModelName.BERT_MODEL_NAME);
		String bertModelCkptPath =
			params.contains(HasModelPath.MODEL_PATH) && (null != params.get(HasModelPath.MODEL_PATH))
				? params.get(HasModelPath.MODEL_PATH)
				: BertResources.getBertModelCkpt(factory, bertModelName);

		String checkpointFilePath = params.get(HasCheckpointFilePathDefaultAsNull.CHECKPOINT_FILE_PATH);
		if (!StringUtils.isNullOrWhitespaceOnly(checkpointFilePath)) {
			userParams.put("model_dir", checkpointFilePath);
		}

		ExternalFilesConfig externalFilesConfig = params.contains(HasUserFiles.USER_FILES)
			? ExternalFilesConfig.fromJson(params.get(HasUserFiles.USER_FILES))
			: new ExternalFilesConfig();

		if (PythonFileUtils.isLocalFile(bertModelCkptPath)) {
			// should be a directory
			userParams.put("pretrained_ckpt_path", bertModelCkptPath.substring("file://".length()));
		} else {
			externalFilesConfig.addFilePaths(bertModelCkptPath);
			userParams.put("pretrained_ckpt_path", PythonFileUtils.getCompressedFileName(bertModelCkptPath));
		}

		Map <String, Map <String, Object>> config = getConfig(getParams(), false, factory);
		String configJson = JsonConverter.toJson(config);

		LOG.info("EasyTransfer config: {}", configJson);
		if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
			System.out.println("EasyTransfer config: " + configJson);
		}
		BertTaskName taskName = params.get(HasTaskName.TASK_NAME);
		userParams.put("app_name", taskName.name());

		EasyTransferConfigTrainBatchOp trainBatchOp = new EasyTransferConfigTrainBatchOp()
			.setSelectedCols(ArrayUtils.add(SAFE_MODEL_INPUTS, labelCol))
			.setConfigJson(configJson)
			.setUserFiles(externalFilesConfig)
			.setUserParams(JsonConverter.toJson(userParams))
			.setNumWorkers(params.get(HasNumWorkersDefaultAsNull.NUM_WORKERS))
			.setNumPSs(params.get(HasNumPssDefaultAsNull.NUM_PSS))
			.setPythonEnv(params.get(HasPythonEnv.PYTHON_ENV))
			.setIntraOpParallelism(params.get(HasIntraOpParallelism.INTRA_OP_PARALLELISM))
			.setMLEnvironmentId(getMLEnvironmentId());

		BatchOperator <?>[] tfInputs;
		tfInputs = new BatchOperator <?>[inputs.length];
		tfInputs[0] = in;
		System.arraycopy(inputs, 1, tfInputs, 1, inputs.length - 1);
		BatchOperator <?> tfModel = trainBatchOp.linkFrom(tfInputs);

		String tfOutputSignatureDef = EasyTransferUtils.getTfOutputSignatureDef(taskType);

		MapPartitionOperator <Row, Row> constructModelMapPartitionOperator = tfModel.getDataSet()
			.partitionCustom(new Partitioner <Long>() {
				@Override
				public int partition(Long key, int numPartitions) {
					return 0;
				}
			}, 0)
			.mapPartition(new ConstructModelMapPartitionFunction(params, SAFE_MODEL_INPUTS,
				tfOutputSignatureDef, TF_OUTPUT_SIGNATURE_TYPE, preprocessPipelineModelSchemaStr))
			// Assume the pipline model is smaller than the TF model
			.withBroadcastSet(preprocessPipelineModelOp.getDataSet(), PREPROCESS_PIPELINE_MODEL_BC_NAME);
		DataSet <Row> modelDataSet = TaskType.CLASSIFICATION.equals(taskType)
			? constructModelMapPartitionOperator.withBroadcastSet(sortedLabels, SORTED_LABELS_BC_NAME)
			: constructModelMapPartitionOperator;

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
