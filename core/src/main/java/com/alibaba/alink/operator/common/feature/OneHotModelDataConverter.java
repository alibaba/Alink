package com.alibaba.alink.operator.common.feature;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import com.alibaba.alink.common.model.SimpleModelDataConverter;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.feature.OneHotTrainBatchOp;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;

/**
 * This converter can help serialize and deserialize the model data.
 */
public class OneHotModelDataConverter extends SimpleModelDataConverter<OneHotModelData, OneHotModelData> {

    /**
     * Serialize the model data to "Tuple2<Params, List<String>>".
     *
     * @param modelData The model data to serialize.
     * @return The serialization result.
     */
    @Override
    public Tuple2<Params, Iterable<String>> serializeModel(OneHotModelData modelData) {
        Params meta = modelData.meta;
        return Tuple2.of(meta, Collections.singletonList(JsonConverter.toJson(modelData.data)));
    }

    /**
     * Deserialize the model data.
     *
     * @param meta      The model meta data.
     * @param modelData The model data.
     * @return The model data used by mapper.
     */
    @Override
    public OneHotModelData deserializeModel(Params meta, Iterable<String> modelData) {
        OneHotModelData retData = new OneHotModelData();
        retData.meta = meta;

        List<String> data = JsonConverter.fromJson(modelData.iterator().next(),
            new TypeReference<List<String>>() {}.getType());

        retData.mapping = new HashMap<>(0);
        // if the data to be encoding has element not in mapping, then give a new mapping value : n + 1.
        retData.otherMapping = data.size();

        for (String row : data) {
            String[] content = row.split(OneHotTrainBatchOp.DELIMITER, 3);
            HashMap<String, Integer> map = retData.mapping.get(content[0]);
            if (map != null) {
                map.put(content[1], Integer.parseInt(content[2]));
            } else {
                HashMap<String, Integer> tmpHash = new LinkedHashMap<>(0);
                tmpHash.put(content[1], Integer.parseInt(content[2]));
                retData.mapping.put(content[0], tmpHash);
            }
        }
        return retData;
    }
}
