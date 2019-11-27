package com.alibaba.alink.operator.common.classification;

import java.util.Arrays;
import java.util.Collections;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.model.LabeledModelDataConverter;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.classification.NaiveBayesTrainParams;
import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;

import com.google.common.collect.Iterables;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * This converter can help serialize and deserialize the model data.
 */
public class NaiveBayesModelDataConverter extends
    LabeledModelDataConverter<NaiveBayesTrainModelData, NaiveBayesPredictModelData> {

    public NaiveBayesModelDataConverter() {}

    public NaiveBayesModelDataConverter(TypeInformation labelType) {
        super(labelType);
    }

    /**
     * Deserialize the model data.
     *
     * @param meta           The model meta data.
     * @param data           The model data.
     * @param distinctLabels The labels.
     * @return The model data used by mapper.
     */
    @Override
    public NaiveBayesPredictModelData deserializeModel(Params meta, Iterable<String> data, Iterable<Object> distinctLabels) {
        NaiveBayesPredictModelData modelData = new NaiveBayesPredictModelData();
        modelData.meta = meta;
        String json = data.iterator().next();
        NaiveBayesProbInfo dataInfo = JsonConverter.fromJson(json, NaiveBayesProbInfo.class);
        modelData.pi = dataInfo.piArray;
        modelData.theta = dataInfo.theta;
        modelData.label = Iterables.toArray(distinctLabels, Object.class);
        modelData.featureNames = modelData.meta.get(HasFeatureColsDefaultAsNull.FEATURE_COLS);
        modelData.modelType = BayesType.valueOf(modelData.meta.get(NaiveBayesTrainParams.MODEL_TYPE));
        modelData.featLen = modelData.theta.numCols();

        int rowSize = modelData.theta.numRows();
        modelData.phi = new double[rowSize];
        modelData.minMat = new DenseMatrix(rowSize, modelData.featLen);
        //construct special model data for the bernoulli model.
        if (BayesType.BERNOULLI.equals(modelData.modelType)) {
            for (int i = 0; i < rowSize; ++i) {
                for (int j = 0; j < modelData.featLen; ++j) {
                    double tmp = Math.log(1 - Math.exp(modelData.theta.get(i, j)));
                    modelData.phi[i] += tmp;
                    modelData.minMat.set(i, j, modelData.theta.get(i, j) - tmp);
                }
            }
        }
        return modelData;
    }

    /**
     * Serialize the model data to "Tuple3<Params, List<String>, List<Object>>".
     *
     * @param modelData The model data to serialize.
     * @return The serialization result.
     */
    @Override
    public Tuple3<Params, Iterable<String>, Iterable<Object>> serializeModel(NaiveBayesTrainModelData modelData) {
        Params meta = new Params()
            .set(NaiveBayesTrainParams.MODEL_TYPE, modelData.modelType.name())
            .set(HasFeatureColsDefaultAsNull.FEATURE_COLS, modelData.featureNames);
        NaiveBayesProbInfo data = new NaiveBayesProbInfo();
        data.piArray = modelData.pi;
        data.theta = modelData.theta;
        return Tuple3.of(meta, Collections.singletonList(JsonConverter.toJson(data)), Arrays.asList(modelData.label));
    }

    public enum BayesType {

        /**
         * Multinomial type.
         */
        MULTINOMIAL,

        /**
         * Bernoulli type.
         */
        BERNOULLI
    }

    public static class NaiveBayesProbInfo {
        /**
         * the pi array.
         */
        public double[] piArray = null;
        /**
         * the probability matrix.
         */
        public DenseMatrix theta;
    }
}
