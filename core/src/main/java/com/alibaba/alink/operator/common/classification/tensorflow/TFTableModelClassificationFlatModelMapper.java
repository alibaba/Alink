package com.alibaba.alink.operator.common.classification.tensorflow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.dl.plugin.TFPredictorClassLoaderFactory;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.mapper.FlatModelMapper;
import com.alibaba.alink.common.mapper.IterableModelLoader;
import com.alibaba.alink.common.model.LabeledModelDataConverter;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.tensorflow.CachedRichModelMapper;
import com.alibaba.alink.operator.common.tensorflow.TFModelDataConverterUtils;
import com.alibaba.alink.operator.common.tensorflow.TFTableModelPredictFlatModelMapper;
import com.alibaba.alink.params.classification.TFTableModelClassificationPredictParams;
import com.alibaba.alink.params.dl.HasInferBatchSizeDefaultAs256;
import com.alibaba.alink.params.tensorflow.savedmodel.TFTableModelPredictParams;
import com.alibaba.alink.pipeline.LocalPredictor;
import com.alibaba.alink.pipeline.LocalPredictorLoader;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TFTableModelClassificationFlatModelMapper extends CachedRichModelMapper implements IterableModelLoader {

	private final TypeInformation <?> labelType;
	private final String predCol;
	private final Map <Object, Double> predDetail = new HashMap <>();
	private List <Object> sortedLabels;
	private LocalPredictor preprocessLocalPredictor = null;
	private TFTableModelPredictFlatModelMapper tfFlatModelMapper;
	private int predColId;
	private boolean isOutputLogits = false;
	private final TFPredictorClassLoaderFactory factory;

	public TFTableModelClassificationFlatModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		predCol = params.get(TFTableModelClassificationPredictParams.PREDICTION_COL);
		labelType = LabeledModelDataConverter.extractLabelType(modelSchema);
		factory = new TFPredictorClassLoaderFactory();
	}

	@Override
	public void open() {
		tfFlatModelMapper.open();
	}

	@Override
	public void close() {
		tfFlatModelMapper.close();
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		TFTableModelClassificationModelDataConverter modelDataConverter =
			new TFTableModelClassificationModelDataConverter(labelType);
		TFTableModelClassificationModelData modelData = modelDataConverter.load(modelRows);
		loadFromModelData(modelData, modelDataConverter.getModelSchema());
	}

	@Override
	public void loadIterableModel(Iterable <Row> modelRowsIterable) {
		TFTableModelClassificationModelDataConverter modelDataConverter =
			new TFTableModelClassificationModelDataConverter(labelType);
		TFTableModelClassificationModelData modelData = modelDataConverter.loadIterable(modelRowsIterable);
		loadFromModelData(modelData, modelDataConverter.getModelSchema());
	}

	protected void loadFromModelData(TFTableModelClassificationModelData modelData, TableSchema modelSchema) {
		Params meta = modelData.getMeta();

		String tfOutputSignatureDef = meta.get(TFModelDataConverterUtils.TF_OUTPUT_SIGNATURE_DEF);
		//TypeInformation<?> tfOutputSignatureType = meta.get(TFModelDataConverterUtils.TF_OUTPUT_SIGNATURE_TYPE);
		TypeInformation <?> tfOutputSignatureType = AlinkTypes.FLOAT_TENSOR;

		TableSchema dataSchema = getDataSchema();
		if (CollectionUtils.isNotEmpty(modelData.getPreprocessPipelineModelRows())) {
			String preprocessPipelineModelSchemaStr = modelData.getPreprocessPipelineModelSchemaStr();
			TableSchema pipelineModelSchema = TableUtil.schemaStr2Schema(preprocessPipelineModelSchemaStr);

			try {
				preprocessLocalPredictor = LocalPredictorLoader.load(
					modelData.getPreprocessPipelineModelRows(), pipelineModelSchema, dataSchema);
			} catch (Exception e) {
				throw new RuntimeException("Cannot initialize preprocess PipelineModel", e);
			}
			dataSchema = preprocessLocalPredictor.getOutputSchema();
		}

		String[] tfInputCols = meta.get(TFModelDataConverterUtils.TF_INPUT_COLS);
		Params tfModelMapperParams = new Params();
		tfModelMapperParams.set(TFTableModelPredictParams.RESERVED_COLS, new String[] {});
		tfModelMapperParams.set(TFTableModelPredictParams.OUTPUT_SIGNATURE_DEFS,
			new String[] {tfOutputSignatureDef});
		tfModelMapperParams.set(TFTableModelPredictParams.OUTPUT_SCHEMA_STR,
			TableUtil.schema2SchemaStr(TableSchema.builder().field(predCol, tfOutputSignatureType).build()));
		tfModelMapperParams.set(TFTableModelPredictParams.SELECTED_COLS, tfInputCols);
		if (meta.contains(HasInferBatchSizeDefaultAs256.INFER_BATCH_SIZE)) {
			tfModelMapperParams.set(HasInferBatchSizeDefaultAs256.INFER_BATCH_SIZE,
				meta.get(HasInferBatchSizeDefaultAs256.INFER_BATCH_SIZE));
		}
		tfModelMapperParams.set(HasInferBatchSizeDefaultAs256.INFER_BATCH_SIZE,
			params.get(HasInferBatchSizeDefaultAs256.INFER_BATCH_SIZE));

		tfFlatModelMapper = new TFTableModelPredictFlatModelMapper(modelSchema, dataSchema, tfModelMapperParams, factory);
		if (null != modelData.getTfModelZipPath()) {
			tfFlatModelMapper.loadModelFromZipFile(modelData.getTfModelZipPath());
		} else {
			tfFlatModelMapper.loadModel(modelData.getTfModelRows());
		}

		predColId = TableUtil.findColIndex(tfFlatModelMapper.getOutputSchema(), predCol);

		sortedLabels = modelData.getSortedLabels();
		isOutputLogits = meta.get(TFModelDataConverterUtils.IS_OUTPUT_LOGITS);
	}

	@Override
	public FlatModelMapper createNew(List <Row> newModelRows) {
		tfFlatModelMapper.loadModel(newModelRows);
		return this;
	}

	@Override
	public void flatMap(Row row, Collector <Row> output) throws Exception {
		PredictionCollector predictionCollector = new PredictionCollector(Row.copy(row), output);
		if (null != preprocessLocalPredictor) {
			row = preprocessLocalPredictor.map(row);
		}
		tfFlatModelMapper.flatMap(row, predictionCollector);
	}

	@Override
	protected Object extractPredictResult(Row row) throws Exception {
		return extractPredictResultDetail(row).f0;
	}

	@Override
	protected Tuple2 <Object, String> extractPredictResultDetail(Row output) throws Exception {
		FloatTensor tensor = (FloatTensor) output.getField(predColId);
		Object predLabel = PredictionExtractUtils.extractFromTensor(tensor, sortedLabels, predDetail, isOutputLogits);
		return Tuple2.of(predLabel, JsonConverter.toJson(predDetail));
	}
}
