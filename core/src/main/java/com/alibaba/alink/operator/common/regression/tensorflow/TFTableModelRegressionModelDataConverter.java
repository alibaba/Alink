package com.alibaba.alink.operator.common.regression.tensorflow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.model.LabeledModelDataConverter;
import com.alibaba.alink.operator.common.tensorflow.TFModelDataConverterUtils;

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
		return TFModelDataConverterUtils.serializeRegressionModel(modelData);
	}

	@Override
	public TFTableModelRegressionModelData deserializeModel(Params meta, Iterable <String> data,
															Iterable <Object> distinctLabels) {
		TFTableModelRegressionModelData modelData = new TFTableModelRegressionModelData();
		TFModelDataConverterUtils.deserializeRegressionModel(modelData, meta, data);
		return modelData;
	}
}
