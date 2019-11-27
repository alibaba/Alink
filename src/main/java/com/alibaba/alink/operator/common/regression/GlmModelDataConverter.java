package com.alibaba.alink.operator.common.regression;

import com.alibaba.alink.common.utils.JsonConverter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.model.SimpleModelDataConverter;
import com.alibaba.alink.params.regression.GlmTrainParams;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * GLM model convert.
 */
public class GlmModelDataConverter extends SimpleModelDataConverter<GlmModelData, GlmModelData> {

    /**
     * default constructor.
     */
    public GlmModelDataConverter() {
    }

    /**
     *
     * @param data: GlmModelData
     * @return model meta and model data.
     */
    @Override
    public Tuple2<Params, Iterable<String>> serializeModel(GlmModelData data) {
        List<String> modelData = new ArrayList<>();
        modelData.add(JsonConverter.toJson(data.coefficients));
        modelData.add(JsonConverter.toJson(data.intercept));
        modelData.add(JsonConverter.toJson(data.diagInvAtWA));
        return Tuple2.of(BuildMeta(data), modelData);
    }

    /**
     *
     * @param meta The model meta data.
     * @param data The model concrete data.
     * @return GlmModelData
     */
    @Override
    public GlmModelData deserializeModel(Params meta, Iterable<String> data) {
        GlmModelData modelData = new GlmModelData();
        modelData.featureColNames = meta.get(GlmTrainParams.FEATURE_COLS);
        modelData.offsetColName = meta.get(GlmTrainParams.OFFSET_COL);
        modelData.weightColName = meta.get(GlmTrainParams.WEIGHT_COL);
        modelData.labelColName = meta.get(GlmTrainParams.LABEL_COL);

        modelData.familyName = meta.get(GlmTrainParams.FAMILY);
        modelData.variancePower = meta.get(GlmTrainParams.VARIANCE_POWER);
        modelData.linkName = meta.get(GlmTrainParams.LINK);
        modelData.linkPower = meta.get(GlmTrainParams.LINK_POWER);
        modelData.fitIntercept = meta.get(GlmTrainParams.FIT_INTERCEPT);
        modelData.regParam = meta.get(GlmTrainParams.REG_PARAM);
        modelData.numIter = meta.get(GlmTrainParams.MAX_ITER);
        modelData.epsilon = meta.get(GlmTrainParams.EPSILON);

        Iterator<String> dataIterator = data.iterator();

        modelData.coefficients = JsonConverter.fromJson(dataIterator.next(), double[].class);
        modelData.intercept = JsonConverter.fromJson(dataIterator.next(), double.class);
        modelData.diagInvAtWA = JsonConverter.fromJson(dataIterator.next(), double[].class);
        return modelData;
    }

    /**
     * build meta.
     * @param modelData: model data/
     * @return params.
     */
    private Params BuildMeta(GlmModelData modelData) {
        return new Params()
            .set(GlmTrainParams.FEATURE_COLS, modelData.featureColNames)
            .set(GlmTrainParams.OFFSET_COL, modelData.offsetColName)
            .set(GlmTrainParams.WEIGHT_COL, modelData.weightColName)
            .set(GlmTrainParams.LABEL_COL, modelData.labelColName)
            .set(GlmTrainParams.FAMILY, modelData.familyName)
            .set(GlmTrainParams.VARIANCE_POWER, modelData.variancePower)
            .set(GlmTrainParams.LINK, modelData.linkName)
            .set(GlmTrainParams.LINK_POWER, modelData.linkPower)
            .set(GlmTrainParams.FIT_INTERCEPT, modelData.fitIntercept)
            .set(GlmTrainParams.REG_PARAM, modelData.regParam)
            .set(GlmTrainParams.EPSILON, modelData.epsilon);
    }
}
