package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.common.utils.JsonConverter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.common.model.SimpleModelDataConverter;
import java.util.Collections;

/**
 * ChiSqSelector model.
 */
public class ChiSqSelectorModelDataConverter extends SimpleModelDataConverter<int[], int[]> {

    public ChiSqSelectorModelDataConverter() {
    }

    /**
     * Serialize the model to "Tuple2<Params, List<String>>"
     *
     * @param modelData: selected col indices
     */
    @Override
    public Tuple2<Params, Iterable<String>> serializeModel(int[] modelData) {
        return Tuple2.of(new Params(), Collections.singletonList(JsonConverter.toJson(modelData)));
    }

    /**
     *
     * @param meta The model meta data.
     * @param modelData: json
     * @return
     */
    @Override
    public int[] deserializeModel(Params meta, Iterable<String> modelData) {
        String json = modelData.iterator().next();
        return JsonConverter.fromJson(json, int[].class);
    }

}
