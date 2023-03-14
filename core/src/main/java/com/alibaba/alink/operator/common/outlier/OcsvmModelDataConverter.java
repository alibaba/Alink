package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.model.SimpleModelDataConverter;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.outlier.OcsvmModelData.SvmModelData;
import com.alibaba.alink.params.outlier.OcsvmModelTrainParams;
import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OcsvmModelDataConverter
	extends SimpleModelDataConverter <OcsvmModelData, OcsvmModelData> {

	public OcsvmModelDataConverter() {
	}

	@Override
	public Tuple2 <Params, Iterable <String>> serializeModel(OcsvmModelData data) {
		Params meta = new Params();
		meta.set(OcsvmModelTrainParams.KERNEL_TYPE, data.kernelType);
		if (data.degree >= 1) {
			meta.set(OcsvmModelTrainParams.DEGREE, data.degree);
		}
		meta.set(OcsvmModelTrainParams.GAMMA, data.gamma);
		meta.set(OcsvmModelTrainParams.COEF0, data.coef0);
		meta.set(OcsvmModelTrainParams.NU, data.nu);
		meta.set(OcsvmModelTrainParams.FEATURE_COLS, data.featureColNames);
		meta.set(OcsvmModelTrainParams.VECTOR_COL, data.vectorCol);
		meta.set(ModelParamName.BAGGING_NUMBER, data.baggingNumber);
		List <String> modelData = new ArrayList <>();
		for (int i = 0; i < data.models.length; ++i) {
			String json = JsonConverter.toJson(data.models[i]);
			modelData.add(json);
		}
		return Tuple2.of(meta, modelData);
	}

	@Override
	public OcsvmModelData deserializeModel(Params meta, Iterable <String> data) {
		OcsvmModelData modelData = new OcsvmModelData();
		modelData.baggingNumber = meta.get(ModelParamName.BAGGING_NUMBER);
		modelData.models = new SvmModelData[modelData.baggingNumber];
		Iterator <String> dataIterator = data.iterator();
		for (int i = 0; i < modelData.baggingNumber; ++i) {
			modelData.models[i] = JsonConverter.fromJson(dataIterator.next(), SvmModelData.class);
		}
		modelData.featureColNames = meta.get(HasSelectedColsDefaultAsNull.SELECTED_COLS);
		modelData.kernelType = meta.get(OcsvmModelTrainParams.KERNEL_TYPE);
		modelData.degree = meta.get(OcsvmModelTrainParams.DEGREE);
		modelData.gamma = meta.get(OcsvmModelTrainParams.GAMMA);
		modelData.coef0 = meta.get(OcsvmModelTrainParams.COEF0);
		modelData.nu = meta.get(OcsvmModelTrainParams.NU);
		modelData.featureColNames = meta.get(OcsvmModelTrainParams.FEATURE_COLS);
		modelData.vectorCol = meta.get(OcsvmModelTrainParams.VECTOR_COL);
		return modelData;
	}
}
