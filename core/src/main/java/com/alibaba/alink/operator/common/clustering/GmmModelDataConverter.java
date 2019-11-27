package com.alibaba.alink.operator.common.clustering;

import com.alibaba.alink.operator.common.clustering.GmmModelData.ClusterSummary;
import com.alibaba.alink.common.model.SimpleModelDataConverter;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.clustering.GmmTrainParams;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;

import java.util.ArrayList;
import java.util.List;

/**
 * Model data converter for Gaussian Mixture model.
 */
public class GmmModelDataConverter extends SimpleModelDataConverter<GmmModelData, GmmModelData> {
    private static final ParamInfo<Integer> NUM_FEATURES = ParamInfoFactory
        .createParamInfo("numFeatures", Integer.class)
        .setRequired()
        .build();

    @Override
    public Tuple2<Params, Iterable<String>> serializeModel(GmmModelData modelData) {
        List<String> data = new ArrayList<>();
        for (ClusterSummary clusterSummary : modelData.data) {
            data.add(JsonConverter.toJson(clusterSummary));
        }
        Params meta = new Params()
            .set(NUM_FEATURES, modelData.dim)
            .set(GmmTrainParams.K, modelData.k)
            .set(GmmTrainParams.VECTOR_COL, modelData.vectorCol);

        return Tuple2.of(meta, data);
    }

    @Override
    public GmmModelData deserializeModel(Params meta, Iterable<String> data) {
        GmmModelData modelData = new GmmModelData();
        modelData.k = meta.get(GmmTrainParams.K);
        modelData.vectorCol = meta.get(GmmTrainParams.VECTOR_COL);
        modelData.dim = meta.get(NUM_FEATURES);
        modelData.data = new ArrayList<>(modelData.k);
        for (String row : data) {
            modelData.data.add(JsonConverter.fromJson(row, ClusterSummary.class));
        }

        return modelData;
    }
}
