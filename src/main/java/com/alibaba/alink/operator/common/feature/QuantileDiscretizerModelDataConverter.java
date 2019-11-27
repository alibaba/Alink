package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.model.SimpleModelDataConverter;
import com.google.common.reflect.TypeToken;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

/**
 * quantile discretizer model data converter.
 */
public class QuantileDiscretizerModelDataConverter
    extends SimpleModelDataConverter<QuantileDiscretizerModelDataConverter, QuantileDiscretizerModelDataConverter> {
    private Map<String, double[]> data;
    private Params meta = new Params();

    public QuantileDiscretizerModelDataConverter() {
    }

    public QuantileDiscretizerModelDataConverter(Map<String, double[]> data, Params meta) {
        this.meta = meta;
        this.data = data;
    }

    @Override
    public Tuple2<Params, Iterable<String>> serializeModel(QuantileDiscretizerModelDataConverter modelData) {
        return Tuple2.of(modelData.meta, Arrays.asList(gson.toJson(this.data)));
    }

    @Override
    public QuantileDiscretizerModelDataConverter deserializeModel(Params meta, Iterable<String> data) {
        this.meta = meta;
        String json = data.iterator().next();
        this.data = gson.fromJson(
            json,
            new TypeToken<HashMap<String, double[]>>() {
            }.getType()
        );

        return this;
    }

    public Map<String, double[]> getData() {
        return data;
    }

    public QuantileDiscretizerModelDataConverter setData(Map<String, double[]> data) {
        this.data = data;
        return this;
    }
}
