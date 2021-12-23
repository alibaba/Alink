package com.alibaba.alink.operator.common.regression.tensorflow;

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
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.tensorflow.CachedRichModelMapper;
import com.alibaba.alink.operator.common.tensorflow.TFModelDataConverterUtils;
import com.alibaba.alink.operator.common.tensorflow.TFTableModelPredictFlatModelMapper;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.params.dl.HasInferBatchSizeDefaultAs256;
import com.alibaba.alink.params.tensorflow.savedmodel.TFTableModelPredictParams;
import com.alibaba.alink.params.regression.TFTableModelRegressionPredictParams;
import com.alibaba.alink.pipeline.LocalPredictor;
import com.alibaba.alink.pipeline.LocalPredictorLoader;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

public class TFTableModelRegressionFlatModelMapper extends CachedRichModelMapper {

	private LocalPredictor preprocessLocalPredictor = null;
	private TFTableModelPredictFlatModelMapper tfFlatModelMapper;

	private final TypeInformation <?> labelType;
	private final String predCol;
	private int predColId;

	public TFTableModelRegressionFlatModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		predCol = params.get(TFTableModelRegressionPredictParams.PREDICTION_COL);
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
		TFTableModelRegressionModelDataConverter modelDataConverter = new TFTableModelRegressionModelDataConverter(
			labelType);
		TFTableModelRegressionModelData modelData = modelDataConverter.load(modelRows);
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
		if (null != modelData.getTfModelZipPath()) {
			tfFlatModelMapper.loadModelFromZipFile(modelData.getTfModelZipPath());
		} else {
			tfFlatModelMapper.loadModel(modelData.getTfModelRows());
		}
		predColId = TableUtil.findColIndex(tfFlatModelMapper.getOutputSchema(), predCol);
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
		FloatTensor tensor = (FloatTensor) row.getField(predColId);
		Preconditions.checkArgument(tensor.size() == 1, "The prediction tensor must have size 1");
		return (double) (tensor.shape().length == 1
			? tensor.getFloat(0)
			: tensor.getFloat());
	}

	@Override
	protected Tuple2 <Object, String> extractPredictResultDetail(Row output) throws Exception {
		throw new UnsupportedOperationException("Not supported predict with details in TFTableModelRegressionFlatModelMapper");
	}
}
