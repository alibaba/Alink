package com.alibaba.alink.operator.common.fm;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.model.LabeledModelDataConverter;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.FmDataFormat;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.Task;

import java.util.Arrays;
import java.util.Collections;

/**
 * Fm model converter. This converter can help serialize and deserialize the model data.
 */
public class FmModelDataConverter extends LabeledModelDataConverter <FmModelData, FmModelData> {

	public FmModelDataConverter() {
		this(null);
	}

	/**
	 * @param labelType label type.
	 */
	public FmModelDataConverter(TypeInformation labelType) {
		super(labelType);
	}

	/**
	 * @param modelData The model data to serialize.
	 * @return
	 */
	@Override
	protected Tuple3 <Params, Iterable <String>, Iterable <Object>> serializeModel(FmModelData modelData) {
		Params meta = new Params()
			.set(ModelParamName.VECTOR_COL_NAME, modelData.vectorColName)
			.set(ModelParamName.LABEL_COL_NAME, modelData.labelColName)
			.set(ModelParamName.TASK, modelData.task.toString())
			.set(ModelParamName.VECTOR_SIZE, modelData.vectorSize)
			.set(ModelParamName.FEATURE_COL_NAMES, modelData.featureColNames)
			.set(ModelParamName.LABEL_VALUES, modelData.labelValues)
			.set(ModelParamName.DIM, modelData.dim)
			.set(ModelParamName.LOSS_CURVE, modelData.convergenceInfo);
		FmDataFormat factors = modelData.fmModel;

		return Tuple3.of(meta, Collections.singletonList(JsonConverter.toJson(factors)),
			Arrays.asList(modelData.labelValues));
	}

	/**
	 * @param meta           The model meta data.
	 * @param data           The model concrete data.
	 * @param distinctLabels Distinct label values of training data.
	 * @return
	 */
	@Override
	protected FmModelData deserializeModel(Params meta, Iterable <String> data, Iterable <Object> distinctLabels) {
		FmModelData modelData = new FmModelData();
		String json = data.iterator().next();
		modelData.fmModel = JsonConverter.fromJson(json, FmDataFormat.class);
		modelData.vectorColName = meta.get(ModelParamName.VECTOR_COL_NAME);
		modelData.featureColNames = meta.get(ModelParamName.FEATURE_COL_NAMES);
		modelData.labelColName = meta.get(ModelParamName.LABEL_COL_NAME);
		modelData.task = Task.valueOf(meta.get(ModelParamName.TASK));
		modelData.dim = meta.get(ModelParamName.DIM);
		modelData.vectorSize = meta.get(ModelParamName.VECTOR_SIZE);
		modelData.convergenceInfo = meta.get(ModelParamName.LOSS_CURVE);

		if (meta.contains(ModelParamName.LABEL_VALUES)) {
			modelData.labelValues = meta.get(ModelParamName.LABEL_VALUES);
		}

		return modelData;
	}
}
