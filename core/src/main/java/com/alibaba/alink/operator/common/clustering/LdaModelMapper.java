package com.alibaba.alink.operator.common.clustering;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.operator.common.nlp.DocCountVectorizerModelMapper;
import com.alibaba.alink.params.nlp.DocCountVectorizerPredictParams;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.common.clustering.lda.LdaUtil;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.clustering.LdaPredictParams;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Lda model mapper.
 */
public class LdaModelMapper extends RichModelMapper {

    public LdaModelData modelData = new LdaModelData();
    private int documentColIdx;
    private DenseMatrix expELogBeta;
    private DenseMatrix alphaMatrix;
    private int topicNum;
    public int vocabularySize;

    private DocCountVectorizerModelMapper.FeatureType featureType = DocCountVectorizerModelMapper.FeatureType.valueOf("WORD_COUNT");
    private HashMap <String, Tuple2 <Integer, Double>> wordIdWeight;
    private int featureNum;

    /**
     * Constructor.
     * @param modelSchema the model schema.
     * @param dataSchema  the data schema.
     * @param params      the params.
     */
    public LdaModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
        super(modelSchema, dataSchema, params);
        params.set(DocCountVectorizerPredictParams.SELECTED_COL, this.params.get(LdaPredictParams.SELECTED_COL));
        String documentColName = this.params.get(LdaPredictParams.SELECTED_COL);
        this.documentColIdx = TableUtil.findColIndex(dataSchema.getFieldNames(), documentColName);
    }

    /**
     * Get the result doc label col type.
     */
    @Override
    protected TypeInformation initPredResultColType() {
        return Types.LONG;
    }

    private static DenseMatrix vectorToMatrix(Double[] vec) {
        DenseMatrix dm = new DenseMatrix(vec.length, 1);
        for (int i = 0; i < vec.length; i++) {
            dm.set(i, 0, vec[i]);
        }
        return dm;
    }

    /**
     * Load lda model from the list of Row type data.
     *
     * @param modelRows the list of Row type data.
     */
    @Override
    public void loadModel(List <Row> modelRows) {
        LdaModelDataConverter model = new LdaModelDataConverter();
        this.modelData = model.load(modelRows);
        this.vocabularySize = modelData.vocabularySize;
        this.topicNum = modelData.topicNum;
        DenseMatrix gamma = modelData.gamma;
        DenseMatrix wordTopicMatrix;
        if (gamma != null) {
            wordTopicMatrix = getWordTopicMatrixGibbs(vocabularySize, topicNum, gamma, modelData);
        } else {
            wordTopicMatrix = modelData.wordTopicCounts;
        }
        this.expELogBeta = LdaUtil.expDirichletExpectation(wordTopicMatrix).transpose();
        this.alphaMatrix = vectorToMatrix(modelData.alpha);
        featureNum = modelData.list.size();
        this.wordIdWeight = LdaUtil.setWordIdWeightPredict(modelData.list);
    }

    /**
     * Generate the word topic matrix when using gibbs method.
     */
    public static DenseMatrix getWordTopicMatrixGibbs(int vocabularySize, int topicNum, DenseMatrix gamma,
                                                      LdaModelData modelData) {
        DenseMatrix WordTopicMatrix = new DenseMatrix(vocabularySize, topicNum);
        double[] pz = new double[topicNum];
        double topicSum = 0;
        for (int k = 0; k < topicNum; ++k) {
            topicSum += gamma.get(vocabularySize, k);
        }
        for (int k = 0; k < topicNum; ++k) {
            pz[k] = gamma.get(vocabularySize, k) / topicSum;
        }
        double[] tmpPwz = new double[topicNum];
        double tmpSum;
        for (int w = 0; w < vocabularySize; ++w) {
            Arrays.fill(tmpPwz, 0);
            tmpSum = 0;
            for (int k = 0; k < topicNum; ++k) {
                double topicWordFactor = gamma.get(w, k);
                double globalTopicFactor = gamma.get(vocabularySize, k);
                double p = (topicWordFactor + modelData.beta[k]) / (globalTopicFactor + topicNum * modelData.beta[k]);
                tmpPwz[k] = p * pz[k];
                tmpSum += tmpPwz[k];
            }
            for (int k = 0; k < topicNum; ++k) {
                if (tmpSum != 0) {
                    tmpPwz[k] /= tmpSum;
                }
                if (tmpPwz[k] > 1.0) {
                    tmpPwz[k] = 1.0;
                }
                WordTopicMatrix.set(w, k, tmpPwz[k]);
            }
        }
        WordTopicMatrix = WordTopicMatrix.transpose();
        return WordTopicMatrix;
    }

    /**
     * Predict the label topic of the input document.
     */
    @Override
    protected Object predictResult(Row row) throws Exception {
        return this.predictResultDetail(row).f0;
    }

    /**
     * Predict the label and the probability that the document belongs to each topic.
     */
    @Override
    protected Tuple2 <Object, String> predictResultDetail(Row row) throws Exception {
        double minTF = 1.0;
        SparseVector sv = DocCountVectorizerModelMapper.predictSparseVector(
            (String) row.getField(documentColIdx), minTF, wordIdWeight, featureType, featureNum);
        double[] values = LdaUtil.getTopicDistributionMethod(sv, this.expELogBeta,
            this.alphaMatrix, this.topicNum);
        DenseVector dv = new DenseVector(values);
        dv.normalizeEqual(1.0);

        Integer maxIndex = 0;
        double maxValue = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < values.length; i++) {
            if (maxValue < values[i]) {
                maxValue = values[i];
                maxIndex = i;
            }
        }

        return new Tuple2 <>(maxIndex.longValue(), dv.toString());
    }

}
