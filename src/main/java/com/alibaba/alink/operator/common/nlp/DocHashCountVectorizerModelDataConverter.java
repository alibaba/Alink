package com.alibaba.alink.operator.common.nlp;

import com.alibaba.alink.common.model.SimpleModelDataConverter;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.nlp.DocHashCountVectorizerTrainParams;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import java.util.Collections;
import java.util.HashMap;

/**
 * ModelDataConverter for DocHashIDFVectorizer.
 **/
public class DocHashCountVectorizerModelDataConverter
    extends SimpleModelDataConverter<DocHashCountVectorizerModelData, DocHashCountVectorizerModelData> {
    public DocHashCountVectorizerModelDataConverter() {
    }

    @Override
    public Tuple2<Params, Iterable<String>> serializeModel(DocHashCountVectorizerModelData data) {
        Params meta = new Params()
            .set(DocHashCountVectorizerTrainParams.NUM_FEATURES, data.numFeatures)
            .set(DocHashCountVectorizerTrainParams.MIN_TF, data.minTF)
            .set(DocHashCountVectorizerTrainParams.FEATURE_TYPE, data.featureType);
        return Tuple2.of(meta, Collections.singletonList(JsonConverter.toJson(data.idfMap)));
    }

    @Override
    public DocHashCountVectorizerModelData deserializeModel(Params meta, Iterable<String> data) {
        String modelString = data.iterator().next();
        DocHashCountVectorizerModelData modelData = new DocHashCountVectorizerModelData();
        modelData.idfMap = JsonConverter.fromJson(modelString,
            new TypeReference<HashMap<Integer, Double>>() {}.getType());
        modelData.numFeatures = meta.get(DocHashCountVectorizerTrainParams.NUM_FEATURES);
        modelData.minTF = meta.get(DocHashCountVectorizerTrainParams.MIN_TF);
        modelData.featureType = meta.get(DocHashCountVectorizerTrainParams.FEATURE_TYPE);
        return modelData;
    }
}
