package com.alibaba.alink.operator.common.regression.tensorflow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.model.LabeledModelDataConverter;
import com.alibaba.alink.operator.common.tensorflow.TFModelDataConverterUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TFTableModelRegressionModelDataConverter extends
	LabeledModelDataConverter <TFTableModelRegressionModelData, TFTableModelRegressionModelData> {

	public TFTableModelRegressionModelDataConverter() {
	}

	public TFTableModelRegressionModelDataConverter(TypeInformation <?> labelType) {
		super(labelType);
	}

	@Override
	public Tuple3 <Params, Iterable <String>, Iterable <Object>> serializeModel(
		TFTableModelRegressionModelData modelData) {
		Params meta = modelData.getMeta().clone();
		meta.set(TFModelDataConverterUtils.TF_INPUT_COLS, modelData.getTfInputCols());
		meta.set(TFModelDataConverterUtils.TF_OUTPUT_SIGNATURE_DEF, modelData.getTfOutputSignatureDef());
		//meta.set(TFModelDataConverterUtils.TF_OUTPUT_SIGNATURE_TYPE, modelData.getTfOutputSignatureType());
		meta.set(TFModelDataConverterUtils.PREPROCESS_PIPELINE_MODEL_SCHEMA_STR, modelData.getPreprocessPipelineModelSchemaStr());

		List <String> data = new ArrayList <>();

		long pStart = 0, pSize;
		List <Row> tfModelSerialized = modelData.getTfModelRows();
		pSize = TFModelDataConverterUtils.appendModelRows(tfModelSerialized, data);
		meta.set(TFModelDataConverterUtils.TF_MODEL_PARTITION_START, pStart);
		meta.set(TFModelDataConverterUtils.TF_MODEL_PARTITION_SIZE, pSize);
		pStart += pSize;

		List <Row> preprocessPipelineModelRows = modelData.getPreprocessPipelineModelRows();
		pSize = TFModelDataConverterUtils.appendModelRows(preprocessPipelineModelRows, data);
		meta.set(TFModelDataConverterUtils.PREPROCESS_PIPELINE_MODEL_PARTITION_START, pStart);
		meta.set(TFModelDataConverterUtils.PREPROCESS_PIPELINE_MODEL_PARTITION_SIZE, pSize);
		pStart += pSize;

		return Tuple3.of(meta, data, new ArrayList <>());
	}

	@Override
	public TFTableModelRegressionModelData deserializeModel(Params meta, Iterable <String> data,
															Iterable <Object> distinctLabels) {
		TFTableModelRegressionModelData modelData = new TFTableModelRegressionModelData();
		modelData.setMeta(meta);
		modelData.setTfInputCols(meta.get(TFModelDataConverterUtils.TF_INPUT_COLS));
		modelData.setPreprocessPipelineModelSchemaStr(meta.get(TFModelDataConverterUtils.PREPROCESS_PIPELINE_MODEL_SCHEMA_STR));

		Iterator <String> iterator = data.iterator();
		String zipFilePath = TFModelDataConverterUtils.writeModelRowsToFile(iterator,
			meta.get(TFModelDataConverterUtils.TF_MODEL_PARTITION_SIZE));
		modelData.setTfModelZipPath(zipFilePath);

		if (meta.contains(TFModelDataConverterUtils.PREPROCESS_PIPELINE_MODEL_PARTITION_SIZE)) {
			List <Row> preprocessPipelineModelSerialized = TFModelDataConverterUtils.extractModelRows(iterator,
				meta.get(TFModelDataConverterUtils.PREPROCESS_PIPELINE_MODEL_PARTITION_SIZE));
			modelData.setPreprocessPipelineModelRows(preprocessPipelineModelSerialized);
		}

		return modelData;
	}
}
