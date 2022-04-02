package com.alibaba.alink.operator.common.classification.tensorflow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.dl.plugin.TFPredictorClassLoaderFactory;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.mapper.IterableModelLoader;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.mapper.MapperChain;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.common.model.LabeledModelDataConverter;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.tensorflow.TFModelDataConverterUtils;
import com.alibaba.alink.operator.common.tensorflow.TFTableModelPredictModelMapper;
import com.alibaba.alink.params.classification.TFTableModelClassificationPredictParams;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.tensorflow.savedmodel.TFTableModelPredictParams;
import com.alibaba.alink.pipeline.ModelExporterUtils;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TFTableModelClassificationModelMapper extends RichModelMapper implements IterableModelLoader {

	private final List <Mapper> mappers = new ArrayList <>();
	private final Map <Object, Double> predDetail = new HashMap <>();
	private TFTableModelPredictModelMapper tfModelMapper;
	private List <Object> sortedLabels;
	private int predColId;
	private boolean isOutputLogits = false;
	private final TFPredictorClassLoaderFactory factory;

	public TFTableModelClassificationModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		factory = new TFPredictorClassLoaderFactory();
	}

	@Override
	protected Object predictResult(SlicedSelectedSample selection) throws Exception {
		return predictResultDetail(selection).f0;
	}

	@Override
	protected Tuple2 <Object, String> predictResultDetail(SlicedSelectedSample selection) throws Exception {
		Row output = new Row(selection.length());
		selection.fillRow(output);
		for (Mapper mapper : mappers) {
			output = mapper.map(output);
		}

		FloatTensor tensor = (FloatTensor) output.getField(predColId);
		Object predLabel = PredictionExtractUtils.extractFromTensor(tensor, sortedLabels, predDetail, isOutputLogits);
		return Tuple2.of(predLabel, JsonConverter.toJson(predDetail));
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		TypeInformation <?> labelType = LabeledModelDataConverter.extractLabelType(getModelSchema());
		TFTableModelClassificationModelDataConverter modelDataConverter =
			new TFTableModelClassificationModelDataConverter(labelType);
		TFTableModelClassificationModelData modelData = modelDataConverter.load(modelRows);
		loadFromModelData(modelData, modelDataConverter.getModelSchema());
	}

	@Override
	public void loadIterableModel(Iterable <Row> modelRowsIterable) {
		TypeInformation <?> labelType = LabeledModelDataConverter.extractLabelType(getModelSchema());
		TFTableModelClassificationModelDataConverter modelDataConverter =
			new TFTableModelClassificationModelDataConverter(labelType);
		TFTableModelClassificationModelData modelData = modelDataConverter.loadIterable(modelRowsIterable);
		loadFromModelData(modelData, modelDataConverter.getModelSchema());
	}

	protected void loadFromModelData(TFTableModelClassificationModelData modelData, TableSchema modelSchema) {
		Params meta = modelData.getMeta();

		String tfOutputSignatureDef = meta.get(TFModelDataConverterUtils.TF_OUTPUT_SIGNATURE_DEF);
		TypeInformation <?> tfOutputSignatureType = AlinkTypes.FLOAT_TENSOR;
		String[] reservedCols = null == params.get(HasReservedColsDefaultAsNull.RESERVED_COLS)
			? getDataSchema().getFieldNames()
			: params.get(HasReservedColsDefaultAsNull.RESERVED_COLS);

		TableSchema dataSchema = getDataSchema();
		if (CollectionUtils.isNotEmpty(modelData.getPreprocessPipelineModelRows())) {
			String preprocessPipelineModelSchemaStr = modelData.getPreprocessPipelineModelSchemaStr();
			TableSchema pipelineModelSchema = TableUtil.schemaStr2Schema(preprocessPipelineModelSchemaStr);

			MapperChain mapperList = ModelExporterUtils.loadMapperListFromStages(
				modelData.getPreprocessPipelineModelRows(),
				pipelineModelSchema,
				dataSchema);
			mappers.addAll(Arrays.asList(mapperList.getMappers()));
			dataSchema = mappers.get(mappers.size() - 1).getOutputSchema();
		}

		String[] tfInputCols = meta.get(TFModelDataConverterUtils.TF_INPUT_COLS);
		String predCol = params.get(TFTableModelClassificationPredictParams.PREDICTION_COL);

		Params tfModelMapperParams = new Params();
		tfModelMapperParams.set(TFTableModelPredictParams.OUTPUT_SIGNATURE_DEFS,
			new String[] {tfOutputSignatureDef});
		tfModelMapperParams.set(TFTableModelPredictParams.OUTPUT_SCHEMA_STR,
			TableUtil.schema2SchemaStr(TableSchema.builder().field(predCol, tfOutputSignatureType).build()));
		tfModelMapperParams.set(TFTableModelPredictParams.SELECTED_COLS, tfInputCols);
		tfModelMapperParams.set(TFTableModelPredictParams.RESERVED_COLS, reservedCols);

		tfModelMapper = new TFTableModelPredictModelMapper(modelSchema,
			dataSchema, tfModelMapperParams, factory);
		if (null != modelData.getTfModelZipPath()) {
			tfModelMapper.loadModelFromZipFile(modelData.getTfModelZipPath());
		} else {
			tfModelMapper.loadModel(modelData.getTfModelRows());
		}
		mappers.add(tfModelMapper);

		predColId = TableUtil.findColIndex(tfModelMapper.getOutputSchema(), predCol);

		sortedLabels = modelData.getSortedLabels();
		isOutputLogits = meta.get(TFModelDataConverterUtils.IS_OUTPUT_LOGITS);
	}

	@Override
	public ModelMapper createNew(List <Row> newModelRows) {
		tfModelMapper.loadModel(newModelRows);
		return this;
	}

	@Override
	public void open() {
		for (Mapper mapper : mappers) {
			mapper.open();
		}
	}

	@Override
	public void close() {
		for (Mapper mapper : mappers) {
			mapper.close();
		}
	}
}
