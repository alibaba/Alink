package com.alibaba.alink.operator.common.clustering;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.clustering.lda.LdaUtil;
import com.alibaba.alink.operator.common.clustering.lda.LdaEvaluateParams;
import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.model.SimpleModelDataConverter;
import com.alibaba.alink.params.clustering.LdaTrainParams;
import com.alibaba.alink.params.clustering.lda.HasAlphaArray;
import com.alibaba.alink.params.clustering.lda.HasBetaArray;
import com.alibaba.alink.params.clustering.lda.HasVocabularySize;

import java.util.ArrayList;
import java.util.List;

/**
 * This converter can help serialize and deserialize the model data.
 */
public class LdaModelDataConverter extends SimpleModelDataConverter<LdaModelData, LdaModelData> {

	/**
	 * Constructor.
	 */
    public LdaModelDataConverter() {
    }

	/**
	 * Serialize the model data to "Tuple2<Params, Iterable<String>>".
	 *
	 * @param modelData The model data to serialize.
	 * @return The serialization result.
	 */
    @Override
    public Tuple2<Params, Iterable<String>> serializeModel(LdaModelData modelData) {
        if (modelData.gamma != null) {
            return Tuple2.of(BuildMeta(modelData), serializeMatrix(Tuple2.of(modelData.gamma, modelData.list)));
        } else {
            return Tuple2.of(BuildMeta(modelData), serializeMatrix(Tuple2.of(modelData.wordTopicCounts, modelData.list)));
        }
    }

	/**
	 * Deserialize the model data.
	 *
	 * @param meta         The model meta data.
	 * @param data         The model concrete data.
	 * @return The deserialized model data.
	 */
    @Override
    public LdaModelData deserializeModel(Params meta, Iterable<String> data) {
        LdaModelData modelData = new LdaModelData();
        modelData.alpha = meta.get(HasAlphaArray.ALPHA_ARRAY);
        modelData.beta = meta.get(HasBetaArray.BETA_ARRAY);
        modelData.topicNum = meta.get(LdaTrainParams.TOPIC_NUM);
        modelData.vocabularySize = meta.get(HasVocabularySize.VOCABULARY_SIZE);
        modelData.optimizer = meta.get(LdaTrainParams.METHOD);
        modelData.logPerplexity = meta.get(LdaEvaluateParams.LOG_PERPLEXITY);
        modelData.logLikelihood = meta.get(LdaEvaluateParams.LOG_LIKELIHOOD);
        Tuple2<DenseMatrix, List<String>> res = deserializeMatrixAndDocCountData(data);
        modelData.list = res.f1;
        LdaUtil.OptimizerMethod optimizerMethod = LdaUtil.OptimizerMethod.valueOf(modelData.optimizer.toUpperCase());
        switch (optimizerMethod) {
            case EM:
                modelData.gamma = res.f0;
                break;
            case ONLINE:
                modelData.wordTopicCounts = res.f0;
                break;
            default:
                throw new NotImplementedException("Optimizer not support.");
        }
        return modelData;
    }

    private Params BuildMeta(LdaModelData modelData) {
        return new Params()
                .set(HasAlphaArray.ALPHA_ARRAY, modelData.alpha)
                .set(HasBetaArray.BETA_ARRAY, modelData.beta)
                .set(LdaTrainParams.TOPIC_NUM, modelData.topicNum)
                .set(HasVocabularySize.VOCABULARY_SIZE, modelData.vocabularySize)
                .set(LdaTrainParams.METHOD, modelData.optimizer)
                .set(LdaEvaluateParams.LOG_PERPLEXITY, modelData.logPerplexity)
                .set(LdaEvaluateParams.LOG_LIKELIHOOD, modelData.logLikelihood);
    }

    private List<String> serializeMatrix(Tuple2<DenseMatrix, List<String>> inputData) {
        String strMatrix = JsonConverter.toJson(inputData.f0);
        int length = strMatrix.length();
        int size = 1024 * 1024;
        int num = length % size > 0 ? length / size + 1 : length / size;
        List<String> strList = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            strList.add(strMatrix.substring(i * size, Math.min((i + 1) * size, length)));
        }
        if (inputData.f1 != null) {
            strList.addAll(inputData.f1);
        }
        return strList;
    }

    private Tuple2<DenseMatrix, List<String>> deserializeMatrixAndDocCountData(Iterable<String> strList) {
        StringBuilder sbd = new StringBuilder();
        List<String> docCountData = new ArrayList<>();
        for (String s : strList) {
            if (s.startsWith("{\"f0")) {
                docCountData.add(s);
            } else {
                sbd.append(s);
            }
        }
        DenseMatrix matrix = JsonConverter.fromJson(sbd.toString(), DenseMatrix.class);
        return Tuple2.of(matrix, docCountData);

    }

}
