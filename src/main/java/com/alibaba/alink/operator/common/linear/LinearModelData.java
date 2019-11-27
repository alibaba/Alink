package com.alibaba.alink.operator.common.linear;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter.ModelData;
import com.alibaba.alink.params.shared.colname.HasVectorCol;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Base linear model contains the common model info of classification and regression.
 *
 */
public class LinearModelData implements Serializable {
	public String[] featureNames;
	public String[] featureTypes;//
	public String vectorColName = null;
	public DenseVector coefVector = null;
	public DenseVector[] coefVectors = null;
	public int vectorSize;
	public String modelName;
	public String labelName;//
	public Object[] labelValues = null;
	public LinearModelType linearModelType;
	public boolean hasInterceptItem = true;
	public double[] lossCurve;//
	public TypeInformation labelType;//

	/**
	 * Null construct function.
	 */
	public LinearModelData() {}

	/**
	 * @param labelType label type.
	 */
	public LinearModelData(TypeInformation labelType) {
		this.labelType = labelType;
	}

	/**
	 * Copy construct function.
	 * @param linearModelData LinearModelData.
	 */
	public LinearModelData(LinearModelData linearModelData) {
		this.featureNames = linearModelData.featureNames;
		this.vectorSize = linearModelData.vectorSize;
		this.vectorColName = linearModelData.vectorColName;
		this.coefVector = linearModelData.coefVector;
		this.coefVectors = linearModelData.coefVectors;
		this.modelName = linearModelData.modelName;
		this.labelValues = linearModelData.labelValues;
		this.linearModelType = linearModelData.linearModelType;
		this.hasInterceptItem = linearModelData.hasInterceptItem;
	}

	/**
	 * Construct function.
	 * @param labelType label Type.
	 * @param meta meta information of model.
	 * @param featureNames the feature column names.
	 * @param coefVector
	 */
	public LinearModelData(TypeInformation labelType, Params meta, String[] featureNames, DenseVector coefVector) {
		this.labelType = labelType;
		this.coefVector = coefVector;
		this.featureNames = featureNames;
		if (meta.contains(ModelParamName.LABEL_VALUES)) {
			this.labelValues = FeatureLabelUtil.recoverLabelType(meta.get(ModelParamName.LABEL_VALUES), this.labelType);
		}
		setMetaInfo(meta);
	}

	public void setMetaInfo(Params meta) {
		this.modelName = meta.get(ModelParamName.MODEL_NAME);
		this.linearModelType = meta.contains(ModelParamName.LINEAR_MODEL_TYPE)
			? meta.get(ModelParamName.LINEAR_MODEL_TYPE) : null;
		this.hasInterceptItem = meta.contains(ModelParamName.HAS_INTERCEPT_ITEM) ? meta.get(
			ModelParamName.HAS_INTERCEPT_ITEM) : true;
		this.vectorSize = meta.contains(ModelParamName.VECTOR_SIZE) ? meta.get(ModelParamName.VECTOR_SIZE) : 0;
		this.vectorColName = meta.contains(HasVectorCol.VECTOR_COL) ? meta.get(HasVectorCol.VECTOR_COL) : null;
	}

	public void setModelData(ModelData modelData) {
		this.featureNames = modelData.featureColNames;
		this.featureTypes = modelData.featureColTypes;
		this.coefVector = modelData.coefVector;
		this.coefVectors = modelData.coefVectors;
	}

	//todo: only usage in loadOldFromatModel
	public void deserializeModel(Params meta, List <String> data, List <Object> distinctLabels) {
		setMetaInfo(meta);
		if (data.size() != 1) {
			throw new RuntimeException("Not valid model.");
		}
		if (distinctLabels.size() > 0) {
			this.labelValues = new Object[distinctLabels.size()];
			for (int i = 0; i < distinctLabels.size(); i++) {
				this.labelValues[i] = distinctLabels.get(i);
			}
		}
		setModelData(JsonConverter.fromJson(data.get(0), ModelData.class));
	}

	//todo: to delete old model
	public void loadOldFromatModel(List <Row> rows) {
		if (rows.get(0).getArity() == 4) { // old model format
			int m = rows.size();
			String metaStr = "";
			String[] strs = new String[m];
			for (Row row : rows) {
				int idx = ((Long) row.getField(0)).intValue();
				if (idx == 0L) {
					metaStr = (String) row.getField(1);
				} else {
					strs[idx - 1] = (String) row.getField(1);
				}
			}
			StringBuilder sbd = new StringBuilder();
			for (int i = 0; i < m; i++) {
				if (null == strs[i]) {
					break;
				}
				sbd.append(strs[i]);
			}
			Params meta = Params.fromJson(metaStr);
			List <String> data = Arrays.asList(sbd.toString());
			meta.set(ModelParamName.IS_OLD_FORMAT, true);
			deserializeModel(meta, data, recoverLabelsFromOldFormatModel(meta));
			return;
		} else {
			throw new RuntimeException("Not old format model");
		}
	}

	//todo: to delete old model
	private List <Object> recoverLabelsFromOldFormatModel(Params meta) {

		this.labelType = FlinkTypeConverter.getFlinkType(meta.get(ModelParamName.LABEL_TYPE_NAME));
		List <Object> labels = new ArrayList<>();

		if (meta.contains(ModelParamName.LABEL_VALUES)) {
			Object[] labelValues = FeatureLabelUtil.recoverLabelType(meta.get(ModelParamName.LABEL_VALUES), labelType);
			labels = Arrays.asList(labelValues);
		}
		return labels;
	}
}
