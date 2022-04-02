package com.alibaba.alink.operator.common.tree;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.model.LabeledModelDataConverter;

import java.util.ArrayList;
import java.util.Arrays;

public class XGBoostModelDataConverter
	extends LabeledModelDataConverter <XGBoostModelDataConverter, XGBoostModelDataConverter> {

	public static final ParamInfo <Integer> XGBOOST_VECTOR_SIZE = ParamInfoFactory
		.createParamInfo("xgboostVectorSize", Integer.class)
		.setRequired()
		.build();

	public Params meta;
	public Iterable <String> modelData;
	public Object[] labels;

	public XGBoostModelDataConverter() {
		this(null);
	}

	public XGBoostModelDataConverter(TypeInformation <?> labelType) {
		super(labelType);
	}

	@Override
	protected Tuple3 <Params, Iterable <String>, Iterable <Object>> serializeModel(
		XGBoostModelDataConverter modelData) {
		return Tuple3.of(meta, this.modelData, labels == null ? null : Arrays.asList(labels));
	}

	@Override
	protected XGBoostModelDataConverter deserializeModel(
		Params meta, Iterable <String> data, Iterable <Object> distinctLabels) {

		this.meta = meta;
		this.modelData = data;

		ArrayList <Object> labelsList = new ArrayList <>();
		distinctLabels.forEach(labelsList::add);
		labels = labelsList.toArray(new Object[0]);

		return this;
	}
}
