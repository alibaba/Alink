package com.alibaba.alink.operator.common.linear;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.model.LabeledModelDataConverter;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorCol;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;


/**
 * Base linear model contains the common model info of classification and regression.
 *
 */
public class LinearModelDataConverter extends LabeledModelDataConverter<LinearModelData, LinearModelData> {
    public LinearModelDataConverter() {
        this(null);
    }

    /**
     * @param labelType label type.
     */
    public LinearModelDataConverter(TypeInformation labelType) {
        super(labelType);
    }

    /**
     * Serialize the model data to "Tuple3<Params, List<String>, Iterable<Object>>".
     *
     * @param modelData The model data to serialize.
     * @return The serialization result.
     */
    @Override
    public Tuple3<Params, Iterable<String>, Iterable<Object>> serializeModel(LinearModelData modelData) {
        List<String> data = new ArrayList<>();
        data.add(JsonConverter.toJson(getModelData(modelData)));
        return Tuple3.of(getMetaInfo(modelData), data,
            modelData.labelValues == null? null: Arrays.asList(modelData.labelValues));
    }

    /**
     * Deserialize the model data.
     *
     * @param meta         The model meta data.
     * @param data         The model concrete data.
     * @param distinctLabels All the label values in the data set.
     * @return The deserialized model data.
     */
    @Override
    public LinearModelData deserializeModel(Params meta, Iterable<String> data, Iterable<Object> distinctLabels) {
        LinearModelData modelData = new LinearModelData();
        if (meta.contains(ModelParamName.LABEL_VALUES)) {
            modelData.labelValues = FeatureLabelUtil.recoverLabelType(meta.get(ModelParamName.LABEL_VALUES),
                this.labelType);
        }
        setMetaInfo(meta, modelData);
        if (distinctLabels != null) {
            List<Object> labelList = new ArrayList<>();
            distinctLabels.forEach(labelList::add);
            modelData.labelValues = labelList.toArray();
        }
        setModelData(JsonConverter.fromJson(data.iterator().next(), ModelData.class), modelData);
        return modelData;
    }

    //todo: this is for old model.
    @Override
    public LinearModelData load(List<Row> rows) {
        if (rows.get(0).getArity() == 4) { // old format model, still used in PAI online learning
            LinearModelData modelData = new LinearModelData();
            modelData.loadOldFromatModel(rows);
            return modelData;
        } else {
            return super.load(rows);
        }
    }

    private Params getMetaInfo(LinearModelData data) {
        Params meta = new Params();
        meta.set(ModelParamName.MODEL_NAME, data.modelName);
        meta.set(ModelParamName.HAS_INTERCEPT_ITEM, data.hasInterceptItem);
        meta.set(ModelParamName.LINEAR_MODEL_TYPE, data.linearModelType);
        if (data.vectorColName != null) {
            meta.set(HasVectorCol.VECTOR_COL, data.vectorColName);
            meta.set(ModelParamName.VECTOR_SIZE, data.vectorSize);
        }
        meta.set(HasLabelCol.LABEL_COL, data.labelName);
        return meta;
    }

    /**
     * Set the meta information into the linear model data.
     */
    private void setMetaInfo(Params meta, LinearModelData data) {
        data.modelName = meta.get(ModelParamName.MODEL_NAME);
        data.linearModelType
            = meta.contains(ModelParamName.LINEAR_MODEL_TYPE) ? meta.get(ModelParamName.LINEAR_MODEL_TYPE) : null;
        data.hasInterceptItem
            = meta.contains(ModelParamName.HAS_INTERCEPT_ITEM) ? meta.get(ModelParamName.HAS_INTERCEPT_ITEM) : true;
        data.vectorSize = meta.contains(ModelParamName.VECTOR_SIZE) ? meta.get(ModelParamName.VECTOR_SIZE) : 0;
        data.vectorColName = meta.contains(HasVectorCol.VECTOR_COL) ? meta.get(HasVectorCol.VECTOR_COL) : null;
        data.labelName = meta.contains(HasLabelCol.LABEL_COL) ? meta.get(HasLabelCol.LABEL_COL) : null;
    }

    /**
     * Get the coefficient data of linear model.
     */
    private ModelData getModelData(LinearModelData data) {
        ModelData modelData = new ModelData();
        modelData.featureColNames = data.featureNames;
        modelData.featureColTypes = data.featureTypes;
        modelData.coefVector = data.coefVector;
        modelData.coefVectors = data.coefVectors;
        return modelData;
    }

    /**
     * Set the coefficient data into the linear model data.
     */
    private void setModelData(ModelData modelData, LinearModelData data) {
        data.featureNames = modelData.featureColNames;
        data.featureTypes = modelData.featureColTypes;
        data.coefVector = modelData.coefVector;

        if (data.modelName.equals("softmax")) {
            double[] w = modelData.coefVector.getData();
            int K = data.labelValues.length;
            int m = w.length / (K - 1);
            data.coefVectors = new DenseVector[K - 1];
            for (int k = 0; k < K - 1; k++) {
                data.coefVectors[k] = new DenseVector(m);
                for (int i = 0; i < m; i++) {
                    data.coefVectors[k].set(i, w[k * m + i]);
                }
            }
        }
    }

    /**
     * The coefficient data of linear model.
     */
    public static class ModelData {
        public String[] featureColNames = null;
        public String[] featureColTypes = null;
        // coefficient for binary classification.
        public DenseVector coefVector = null;
        // coefficients for multiple classification.
        public DenseVector[] coefVectors = null;
    }
}
