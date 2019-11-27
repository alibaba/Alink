package com.alibaba.alink.operator.common.nlp;

import com.alibaba.alink.common.model.SimpleModelDataConverter;
import com.alibaba.alink.params.nlp.DocCountVectorizerTrainParams;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import java.util.ArrayList;

/**
 * ModelDataConverter for DocCountVectorizer.
 *
 * <p>Record the document frequency(DF), word count(WC) and inverse document frequency(IDF) of every word.
 */
public class DocCountVectorizerModelDataConverter extends SimpleModelDataConverter<DocCountVectorizerModelData, DocCountVectorizerModelData> {
    public DocCountVectorizerModelDataConverter() {
    }

    @Override
    public Tuple2<Params, Iterable<String>> serializeModel(DocCountVectorizerModelData data) {
        Params params = new Params().set(DocCountVectorizerTrainParams.MIN_TF, data.minTF)
            .set(DocCountVectorizerTrainParams.FEATURE_TYPE, data.featureType);
        return Tuple2.of(params, data.list);
    }

    @Override
    public DocCountVectorizerModelData deserializeModel(Params meta, Iterable<String> modelData) {
        DocCountVectorizerModelData data = new DocCountVectorizerModelData();
        data.list = new ArrayList<>();
        modelData.forEach(data.list::add);
        data.minTF = meta.get(DocCountVectorizerTrainParams.MIN_TF);
        data.featureType = meta.get(DocCountVectorizerTrainParams.FEATURE_TYPE);
        return data;
    }
}
