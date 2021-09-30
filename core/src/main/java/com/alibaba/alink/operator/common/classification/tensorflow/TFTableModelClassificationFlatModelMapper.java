package com.alibaba.alink.operator.common.classification.tensorflow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.TensorTypes;
import com.alibaba.alink.common.mapper.FlatModelMapper;
import com.alibaba.alink.common.model.LabeledModelDataConverter;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.tensorflow.CachedRichModelMapper;
import com.alibaba.alink.operator.common.tensorflow.TFModelDataConverterUtils;
import com.alibaba.alink.operator.common.tensorflow.TFTableModelPredictFlatModelMapper;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.params.dl.HasInferBatchSizeDefaultAs256;
import com.alibaba.alink.params.classification.TFTableModelClassificationPredictParams;
import com.alibaba.alink.params.tensorflow.savedmodel.TFTableModelPredictParams;
import com.alibaba.alink.pipeline.LocalPredictor;
import com.alibaba.alink.pipeline.LocalPredictorLoader;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TFTableModelClassificationFlatModelMapper extends CachedRichModelMapper {

	private final TypeInformation <?> labelType;
	private List <Object> sortedLabels;

	private LocalPredictor preprocessLocalPredictor = null;
	private TFTableModelPredictFlatModelMapper tfFlatModelMapper;

	private final String predCol;
	private int predColId;
	private final Map <Object, Double> predDetail = new HashMap <>();

	public TFTableModelClassificationFlatModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		predCol = params.get(TFTableModelClassificationPredictParams.PREDICTION_COL);
		labelType = LabeledModelDataConverter.extractLabelType(modelSchema);
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
		TFTableModelClassificationModelDataConverter
			modelDataConverter = new TFTableModelClassificationModelDataConverter(
			labelType);
		TFTableModelClassificationModelData modelData = modelDataConverter.load(modelRows);
		Params meta = modelData.getMeta();

		String tfOutputSignatureDef = meta.get(TFModelDataConverterUtils.TF_OUTPUT_SIGNATURE_DEF);
		//TypeInformation<?> tfOutputSignatureType = meta.get(TFModelDataConverterUtils.TF_OUTPUT_SIGNATURE_TYPE);
		TypeInformation <?> tfOutputSignatureType = TensorTypes.FLOAT_TENSOR;

		TableSchema dataSchema = getDataSchema();
		if (CollectionUtils.isNotEmpty(modelData.getPreprocessPipelineModelRows())) {
			String preprocessPipelineModelSchemaStr = modelData.getPreprocessPipelineModelSchemaStr();
			TableSchema pipelineModelSchema = CsvUtil.schemaStr2Schema(preprocessPipelineModelSchemaStr);

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
			CsvUtil.schema2SchemaStr(TableSchema.builder().field(predCol, tfOutputSignatureType).build()));
		tfModelMapperParams.set(TFTableModelPredictParams.SELECTED_COLS, tfInputCols);
		if (meta.contains(HasInferBatchSizeDefaultAs256.INFER_BATCH_SIZE)) {
			tfModelMapperParams.set(HasInferBatchSizeDefaultAs256.INFER_BATCH_SIZE,
				meta.get(HasInferBatchSizeDefaultAs256.INFER_BATCH_SIZE));
		}
		tfModelMapperParams.set(HasInferBatchSizeDefaultAs256.INFER_BATCH_SIZE,
			params.get(HasInferBatchSizeDefaultAs256.INFER_BATCH_SIZE));

		tfFlatModelMapper = new TFTableModelPredictFlatModelMapper(modelDataConverter.getModelSchema(),
			dataSchema, tfModelMapperParams);
		tfFlatModelMapper.loadModel(modelData.getTfModelRows());

		predColId = TableUtil.findColIndex(tfFlatModelMapper.getOutputSchema(), predCol);

		sortedLabels = modelData.getSortedLabels();
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
		Preconditions.checkArgument(tensor.shape().length <= 1,
			"The prediction tensor must be rank-0 or rank-1");

		// If the tensor has size 1, the model was trained for binary classification task,
		// and only output the predication probability to be positive.
		Object predLabel;
		if (tensor.size() == 1) {
			double p = (tensor.shape().length == 0)
				? tensor.getFloat()
				: tensor.getFloat(0);
			Object negLabel = sortedLabels.get(0);
			Object posLabel = sortedLabels.get(1);
			predLabel = p >= 0.5 ? posLabel : negLabel;
			predDetail.put(posLabel, p);
			predDetail.put(negLabel, 1 - p);
		} else {
			int maxi = 0;
			for (int i = 0; i < sortedLabels.size(); i += 1) {
				double p = tensor.getFloat(i);
				predDetail.put(sortedLabels.get(i), p);
				if (p > tensor.getFloat(maxi)) {
					maxi = i;
				}
			}
			predLabel = sortedLabels.get(maxi);
		}
		return Tuple2.of(predLabel, JsonConverter.toJson(predDetail));
	}
}
