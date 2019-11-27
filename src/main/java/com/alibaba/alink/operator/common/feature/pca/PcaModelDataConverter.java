package com.alibaba.alink.operator.common.feature.pca;

import com.alibaba.alink.common.model.SimpleModelDataConverter;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;
import com.alibaba.alink.params.feature.PcaTrainParams;

import java.util.Arrays;

public class PcaModelDataConverter extends SimpleModelDataConverter<PcaModelData, PcaModelData> {

    /**
     * pca result
     */
    public PcaModelData pcaResult;

    /**
     * col names
     */
    public String[] featureColNames;

    /**
     * vector column name
     */
    public String vectorColName;

    /**
     * pca type
     */
    public String pcaType;


    public PcaModelDataConverter() {
    }

    /**
     * Serialize the model to "Tuple2<Params, List<String>>"
     *
     * @param modelData
     */
    @Override
    public Tuple2<Params, Iterable<String>> serializeModel(PcaModelData modelData) {
        Params meta = new Params()
            .set(HasFeatureColsDefaultAsNull.FEATURE_COLS, featureColNames)
            .set(PcaTrainParams.VECTOR_COL, vectorColName)
            .set(PcaTrainParams.CALCULATION_TYPE, pcaType);
        return Tuple2.of(meta, Arrays.asList(JsonConverter.toJson(modelData)));
    }

    @Override
    public PcaModelData deserializeModel(Params meta, Iterable<String> data) {
        String json = data.iterator().next();
        return JsonConverter.fromJson(json, PcaModelData.class);
    }

}
