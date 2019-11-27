package com.alibaba.alink.operator.common.nlp;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.mapper.SISOModelMapper;
import com.alibaba.alink.common.utils.Functional.SerializableTriFunction;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.params.nlp.DocCountVectorizerTrainParams;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transform a document to a sparse vector with the statistics of DocCountVectorizerModel.
 *
 * <p>It supports several types: IDF/TF/TF_IDF/Binary/WordCount.
 */
public class DocCountVectorizerModelMapper extends SISOModelMapper {

    /**
     * TypeReference for deserialize data from json string.
     */
    private static final Type DATA_TUPLE3_TYPE = new TypeReference<Tuple3<String, Double, Integer>>() {
    }.getType();

    public enum FeatureType implements Serializable {
        /**
         * IDF type, the output value is inverse document frequency.
         */
        IDF(
            (idf, termFrequency, tokenRatio) -> idf
        ),
        /**
         * WORD_COUNT type, the output value is the word count.
         */
        WORD_COUNT(
            (idf, termFrequency, tokenRatio) -> termFrequency
        ),
        /**
         * TF_IDF type, the output value is term frequency * inverse document frequency.
         */
        TF_IDF(
            (idf, termFrequency, tokenRatio) -> idf * termFrequency * tokenRatio
        ),
        /**
         * BINARY type, the output value is 1.0.
         */
        BINARY(
            (idf, termFrequency, tokenRatio) -> 1.0
        ),
        /**
         * TF type, the output value is term frequency.
         */
        TF(
            (idf, termFrequency, tokenRatio) -> termFrequency * tokenRatio
        );

        final SerializableTriFunction<Double, Double, Double, Double> featureValueFunc;

        FeatureType(SerializableTriFunction<Double, Double, Double, Double> featureValueFunc) {
            this.featureValueFunc = featureValueFunc;
        }
    }

    private double minTF;
    private FeatureType featureType;
    private HashMap<String, Tuple2<Integer, Double>> wordIdWeight;
    private int featureNum;

    public DocCountVectorizerModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
        super(modelSchema, dataSchema, params);
        this.featureType = FeatureType.valueOf(this.params.get(DocCountVectorizerTrainParams.FEATURE_TYPE).toUpperCase());
    }

    @Override
    protected TypeInformation initPredResultColType() {
        return VectorTypes.SPARSE_VECTOR;
    }

    @Override
    public void loadModel(List<Row> modelRows) {
        this.wordIdWeight = new HashMap<>(modelRows.size());
        DocCountVectorizerModelData data = new DocCountVectorizerModelDataConverter().load(modelRows);
        featureNum = data.list.size();
        minTF = data.minTF;
        this.featureType = DocCountVectorizerModelMapper.FeatureType.valueOf(data.featureType.toUpperCase());
        for (String feature : data.list) {
            Tuple3<String, Double, Integer> t = JsonConverter.fromJson(feature, DATA_TUPLE3_TYPE);
            wordIdWeight.put(t.f0, Tuple2.of(t.f2, t.f1));
        }
    }

    @Override
    protected Object predictResult(Object input) {
        if (null == input) {
            return null;
        }
        String content = (String) input;
        return predictSparseVector(content, minTF, wordIdWeight, featureType, featureNum);
    }
    public static SparseVector predictSparseVector(String content, double minTF,
                                                   HashMap<String, Tuple2<Integer, Double>> wordIdWeight,
                                                   FeatureType featureType, int featureNum) {
        HashMap<String, Integer> wordCount = new HashMap<>(0);

        String[] tokens = content.split(NLPConstant.WORD_DELIMITER);
        double minTermCount = minTF >= 1.0 ? minTF : minTF * tokens.length;
        double tokenRatio = 1.0 / tokens.length;

        for (String token : tokens) {
            if (wordIdWeight.containsKey(token)) {
                wordCount.merge(token, 1, Integer::sum);
            }
        }
        int[] indexes = new int[wordCount.size()];
        double[] values = new double[indexes.length];
        int pos = 0;
        for (Map.Entry<String, Integer> entry : wordCount.entrySet()) {
            double count = entry.getValue();
            if (count >= minTermCount) {
                Tuple2<Integer, Double> idWeight = wordIdWeight.get(entry.getKey());
                indexes[pos] = idWeight.f0;
                values[pos++] = featureType.featureValueFunc.apply(idWeight.f1, count, tokenRatio);
            }
        }
        return new SparseVector(featureNum, Arrays.copyOf(indexes, pos), Arrays.copyOf(values, pos));
    }
}

