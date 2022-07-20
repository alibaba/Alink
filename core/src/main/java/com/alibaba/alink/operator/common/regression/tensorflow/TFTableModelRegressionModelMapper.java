package com.alibaba.alink.operator.common.regression.tensorflow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.dl.plugin.TFPredictorClassLoaderFactory;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.mapper.IterableModelLoader;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.mapper.MapperChain;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.common.model.LabeledModelDataConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.tensorflow.TFModelDataConverterUtils;
import com.alibaba.alink.operator.common.tensorflow.TFTableModelPredictModelMapper;
import com.alibaba.alink.params.classification.TFTableModelClassificationPredictParams;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.tensorflow.savedmodel.TFTableModelPredictParams;
import com.alibaba.alink.pipeline.ModelExporterUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TFTableModelRegressionModelMapper extends RichModelMapper implements IterableModelLoader {

	private TFTableModelPredictModelMapper tfModelMapper;
	private final TFPredictorClassLoaderFactory factory;

	List <Mapper> mappers = new ArrayList <>();

	private int predColId;

	public TFTableModelRegressionModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		factory = new TFPredictorClassLoaderFactory();
	}

	@Override
	protected TypeInformation <?> initPredResultColType(TableSchema modelSchema) {
		return Types.DOUBLE;
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

	@Override
	protected Object predictResult(SlicedSelectedSample selection) throws Exception {
		Row output = new Row(selection.length());
		selection.fillRow(output);
		for (Mapper mapper : mappers) {
			output = mapper.map(output);
		}

		FloatTensor tensor = (FloatTensor) output.getField(predColId);
		AkPreconditions.checkState(tensor.size() == 1, "The prediction tensor must have size 1");
		return (double) (tensor.shape().length == 1
			? tensor.getFloat(0)
			: tensor.getFloat());
	}

	@Override
	protected Tuple2 <Object, String> predictResultDetail(SlicedSelectedSample selection) throws Exception {
		throw new UnsupportedOperationException("Not supported predict with details in TFTableModelRegressionModelMapper");
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		TypeInformation <?> labelType = LabeledModelDataConverter.extractLabelType(getModelSchema());
		TFTableModelRegressionModelDataConverter modelDataConverter = new TFTableModelRegressionModelDataConverter(
			labelType);
		TFTableModelRegressionModelData modelData = modelDataConverter.load(modelRows);
		loadFromModelData(modelData, modelDataConverter.getModelSchema());
	}

	@Override
	public void loadIterableModel(Iterable <Row> modelRowsIterable) {
		TypeInformation <?> labelType = LabeledModelDataConverter.extractLabelType(getModelSchema());
		TFTableModelRegressionModelDataConverter modelDataConverter = new TFTableModelRegressionModelDataConverter(
			labelType);
		TFTableModelRegressionModelData modelData = modelDataConverter.loadIterable(modelRowsIterable);
		loadFromModelData(modelData, modelDataConverter.getModelSchema());
	}

	protected void loadFromModelData(TFTableModelRegressionModelData modelData, TableSchema modelSchema) {
		Params meta = modelData.getMeta();

		String tfOutputSignatureDef = meta.get(TFModelDataConverterUtils.TF_OUTPUT_SIGNATURE_DEF);
		//TypeInformation <?> tfOutputSignatureType = meta.get(TFModelDataConverterUtils.TF_OUTPUT_SIGNATURE_TYPE);
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
		String[] tfReservedCols = ArrayUtils.addAll(reservedCols);

		Params tfModelMapperParams = new Params();
		tfModelMapperParams.set(TFTableModelPredictParams.OUTPUT_SIGNATURE_DEFS,
			new String[] {tfOutputSignatureDef});
		tfModelMapperParams.set(TFTableModelPredictParams.OUTPUT_SCHEMA_STR,
			TableUtil.schema2SchemaStr(TableSchema.builder().field(predCol, tfOutputSignatureType).build()));
		tfModelMapperParams.set(TFTableModelPredictParams.SELECTED_COLS, tfInputCols);
		tfModelMapperParams.set(TFTableModelPredictParams.RESERVED_COLS, tfReservedCols);

		tfModelMapper = new TFTableModelPredictModelMapper(modelSchema, dataSchema, tfModelMapperParams, factory);
		if (null != modelData.getTfModelZipPath()) {
			tfModelMapper.loadModelFromZipFile(modelData.getTfModelZipPath());
		} else {
			tfModelMapper.loadModel(modelData.getTfModelRows());
		}
		mappers.add(tfModelMapper);

		predColId = TableUtil.findColIndex(tfModelMapper.getOutputSchema(), predCol);
	}

	@Override
	public ModelMapper createNew(List <Row> newModelRows) {
		tfModelMapper.loadModel(newModelRows);
		return this;
	}
}
