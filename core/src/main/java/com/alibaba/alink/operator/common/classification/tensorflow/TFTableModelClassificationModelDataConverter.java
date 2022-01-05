package com.alibaba.alink.operator.common.classification.tensorflow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.model.LabeledModelDataConverter;
import com.alibaba.alink.operator.common.tensorflow.TFModelDataConverterUtils;

public class TFTableModelClassificationModelDataConverter
	extends LabeledModelDataConverter <TFTableModelClassificationModelData, TFTableModelClassificationModelData> {

	public TFTableModelClassificationModelDataConverter() {
	}

	public TFTableModelClassificationModelDataConverter(TypeInformation <?> labelType) {
		super(labelType);
	}

	@Override
	protected Tuple3 <Params, Iterable <String>, Iterable <Object>> serializeModel(
		TFTableModelClassificationModelData modelData) {
		return TFModelDataConverterUtils.serializeClassificationModel(modelData);
	}

	@Override
	protected TFTableModelClassificationModelData deserializeModel(Params meta, Iterable <String> data,
																   Iterable <Object> distinctLabels) {
		TFTableModelClassificationModelData modelData = new TFTableModelClassificationModelData();
		TFModelDataConverterUtils.deserializeClassificationModel(modelData, meta, data, distinctLabels);
		return modelData;
	}
}
