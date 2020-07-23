package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.common.model.SimpleModelDataConverter;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.feature.BasedChisqSelectorParams;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import java.util.Collections;

/**
 * ChiSqSelector model.
 */
public class ChiSqSelectorModelDataConverter extends SimpleModelDataConverter<ChisqSelectorModelInfo, ChisqSelectorModelInfo> {

    public ChiSqSelectorModelDataConverter() {
    }

    /**
     * Serialize the model to "Tuple2<Params, List<String>>"
     *
     * @param modelInfo: selected col indices
     */
    @Override
    public Tuple2<Params, Iterable<String>> serializeModel(ChisqSelectorModelInfo modelInfo) {
        return Tuple2.of(new Params(), Collections.singletonList(JsonConverter.toJson(modelInfo)));
    }

    /**
     * @param meta       The model meta data.
     * @param modelData: json
     * @return
     */
    @Override
    public ChisqSelectorModelInfo deserializeModel(Params meta, Iterable<String> modelData) {
        String json = modelData.iterator().next();
        return JsonConverter.fromJson(json, ChisqSelectorModelInfo.class);
    }

}
