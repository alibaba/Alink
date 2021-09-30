package com.alibaba.alink.operator.common.classification.tensorflow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.TensorTypes;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.mapper.MapperChain;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.common.model.LabeledModelDataConverter;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.tensorflow.TFModelDataConverterUtils;
import com.alibaba.alink.operator.common.tensorflow.TFTableModelPredictModelMapper;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.params.classification.TFTableModelClassificationPredictParams;
import com.alibaba.alink.params.tensorflow.savedmodel.TFTableModelPredictParams;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.pipeline.ModelExporterUtils;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TFTableModelClassificationModelMapper extends RichModelMapper {

	private TFTableModelPredictModelMapper tfModelMapper;

	private final List <Mapper> mappers = new ArrayList <>();

	private List <Object> sortedLabels;

	private int predColId;
	private final Map <Object, Double> predDetail = new HashMap <>();

	public TFTableModelClassificationModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
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

	@Override
	public void loadModel(List <Row> modelRows) {
		TypeInformation <?> labelType = LabeledModelDataConverter.extractLabelType(getModelSchema());

		TFTableModelClassificationModelDataConverter
			modelDataConverter = new TFTableModelClassificationModelDataConverter(
			labelType);
		TFTableModelClassificationModelData modelData = modelDataConverter.load(modelRows);
		Params meta = modelData.getMeta();

		String tfOutputSignatureDef = meta.get(TFModelDataConverterUtils.TF_OUTPUT_SIGNATURE_DEF);
		TypeInformation <?> tfOutputSignatureType = TensorTypes.FLOAT_TENSOR;
		String[] reservedCols = null == params.get(HasReservedColsDefaultAsNull.RESERVED_COLS)
			? getDataSchema().getFieldNames()
			: params.get(HasReservedColsDefaultAsNull.RESERVED_COLS);

		TableSchema dataSchema = getDataSchema();
		if (CollectionUtils.isNotEmpty(modelData.getPreprocessPipelineModelRows())) {
			String preprocessPipelineModelSchemaStr = modelData.getPreprocessPipelineModelSchemaStr();
			TableSchema pipelineModelSchema = CsvUtil.schemaStr2Schema(preprocessPipelineModelSchemaStr);

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
			CsvUtil.schema2SchemaStr(TableSchema.builder().field(predCol, tfOutputSignatureType).build()));
		tfModelMapperParams.set(TFTableModelPredictParams.SELECTED_COLS, tfInputCols);
		tfModelMapperParams.set(TFTableModelPredictParams.RESERVED_COLS, reservedCols);

		tfModelMapper = new TFTableModelPredictModelMapper(modelDataConverter.getModelSchema(),
			dataSchema, tfModelMapperParams);
		tfModelMapper.loadModel(modelData.getTfModelRows());
		mappers.add(tfModelMapper);

		predColId = TableUtil.findColIndex(tfModelMapper.getOutputSchema(), predCol);

		sortedLabels = modelData.getSortedLabels();
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
