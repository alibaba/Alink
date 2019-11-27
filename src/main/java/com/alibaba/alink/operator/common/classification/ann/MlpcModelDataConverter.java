package com.alibaba.alink.operator.common.classification.ann;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.model.LabeledModelDataConverter;
import com.alibaba.alink.common.utils.JsonConverter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

import java.util.ArrayList;
import java.util.List;

/**
 * The ModelDataConverter for {@link com.alibaba.alink.pipeline.classification.MultilayerPerceptronClassificationModel}.
 */
public class MlpcModelDataConverter extends LabeledModelDataConverter<MlpcModelData, MlpcModelData> {

    public MlpcModelDataConverter() {
    }

    public MlpcModelDataConverter(TypeInformation labelType) {
        super(labelType);
    }

    @Override
    protected Tuple3<Params, Iterable<String>, Iterable<Object>> serializeModel(MlpcModelData modelData) {
        List<String> data = new ArrayList<>();
        data.add(JsonConverter.toJson(modelData.weights));
        return Tuple3.of(modelData.meta, data, modelData.labels);
    }

    @Override
    protected MlpcModelData deserializeModel(Params meta, Iterable<String> data, Iterable<Object> distinctLabels) {
        MlpcModelData modelData = new MlpcModelData();
        modelData.meta = meta;
        modelData.labels = new ArrayList<>();
        distinctLabels.forEach(modelData.labels::add);
        modelData.weights = JsonConverter.fromJson(data.iterator().next(), DenseVector.class);
        return modelData;
    }
}
