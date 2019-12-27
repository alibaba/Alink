package com.alibaba.alink.operator.common.clustering.lda;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.List;

/**
 * Online corpus step.
 * Build the corpus, calculate the word-topic probability information,
 * logPhatPart, no empty word and document count.
 */
public class OnlineCorpusStep extends ComputeFunction {

    private int numTopic;
    private double subSamplingRate;

    /**
     * Constructor.
     * @param numTopic the number of topics.
     * @param subSamplingRate the rate of the doc which need to be sampled.
     */
    public OnlineCorpusStep(int numTopic, double subSamplingRate) {
        this.numTopic = numTopic;
        this.subSamplingRate = subSamplingRate;
    }

    @Override
    public void calc(ComContext context) {
        Tuple2<Long, Integer> tuple2 = ((List<Tuple2<Long, Integer>>) context.getObj(LdaVariable.shape)).get(0);
        int vocabularySize = tuple2.f1;
        List<Vector> data = context.getObj(LdaVariable.data);
        DenseMatrix lambda;
        DenseMatrix alpha;
        //the first iteration
        if (context.getStepNo() == 1) {
            Tuple2<DenseMatrix, DenseMatrix> initGammaAndAlpha = ((List<Tuple2<DenseMatrix, DenseMatrix>>)
                context.getObj(LdaVariable.initModel)).get(0);
            lambda = initGammaAndAlpha.f0;
            alpha = initGammaAndAlpha.f1;
        } else {
            lambda = context.getObj(LdaVariable.lambda);
            alpha = context.getObj(LdaVariable.alpha);
        }
        if (data == null) {
            context.putObj(LdaVariable.wordTopicStat, new double[numTopic * vocabularySize]);
            context.putObj(LdaVariable.logPhatPart, new double[numTopic]);
            context.putObj(LdaVariable.nonEmptyWordCount, new double[]{0});
            context.putObj(LdaVariable.nonEmptyDocCount, new double[]{0});
            return;
        }
        DenseMatrix gammad = null;
        Tuple4<DenseMatrix, DenseMatrix, Long, Long> corpusUpdatedData =
            onlineCorpusUpdate(data, lambda, alpha, gammad,
                vocabularySize, numTopic, subSamplingRate);

        context.putObj(LdaVariable.wordTopicStat, corpusUpdatedData.f0.getData().clone());
        context.putObj(LdaVariable.logPhatPart, corpusUpdatedData.f1.getData().clone());
        context.putObj(LdaVariable.nonEmptyWordCount, new double[]{corpusUpdatedData.f2});
        context.putObj(LdaVariable.nonEmptyDocCount, new double[]{corpusUpdatedData.f3});
    }

    static Tuple4<DenseMatrix, DenseMatrix, Long, Long> onlineCorpusUpdate(
        List<Vector> data, DenseMatrix lambda, DenseMatrix alpha, DenseMatrix gammad,
        int vocabularySize, int numTopic) {
        return onlineCorpusUpdate(data, lambda, alpha, gammad,
            vocabularySize, numTopic, 1.0);
    }

    private static Tuple4<DenseMatrix, DenseMatrix, Long, Long> onlineCorpusUpdate(
        List<Vector> data, DenseMatrix lambda, DenseMatrix alpha, DenseMatrix gammad,
        int vocabularySize, int numTopic, double subSamplingRate) {

        boolean isRandGamma = gammad == null;
        //wordTopicStat is the word topic probability information.
        DenseMatrix wordTopicStat = DenseMatrix.zeros(numTopic, vocabularySize);
        DenseMatrix logPhatPart = new DenseMatrix(numTopic, 1);
        DenseMatrix expELogBeta = LdaUtil.expDirichletExpectation(lambda).transpose();
        long nonEmptyWordCount = 0;
        long nonEmptyDocCount = 0;
        //the online corpus update stage can update the model in two way.
        //if the document order is determined, then it will update in the order.
        //or it will choose documents randomly.
        RandomDataGenerator random = new RandomDataGenerator();
        GammaDistribution distribution = new GammaDistribution(100, 100);
        int dataSize = data.size();
        boolean sampled = false;
        for (int j = 0; j < dataSize; ++j) {
            //if the subSamplingRate is too small and no doc is sampled in one iteration, then will randomly
            //choose one doc to update the model.
            double rate = random.nextUniform(0, 1);
            int index = -1;
            if (rate < subSamplingRate) {
                index = j;
                sampled = true;
            }
            if (j + 1 == dataSize && !sampled) {
                index = random.nextInt(0, dataSize - 1);
            }
            if (index != -1) {
                Vector vec = data.get(index);
                SparseVector sv = (SparseVector) vec;
                sv.setSize(vocabularySize);
                sv.removeZeroValues();
                for (int i = 0; i < sv.numberOfValues(); i++) {
                    nonEmptyWordCount += sv.getValues()[i];
                }
                if (isRandGamma) {
                    if (gammad == null) {
                        gammad = new DenseMatrix(numTopic, 1);
                    }
                    for (int i = 0; i < numTopic; i++) {
                        gammad.set(i, 0, distribution.inverseCumulativeProbability(random.nextUniform(0, 1)));
                    }
                }
                Tuple2<DenseMatrix, DenseMatrix> topicDistributionTuple =
                    LdaUtil.getTopicDistributionMethod(sv, expELogBeta, alpha, gammad, numTopic);

                for (int i = 0; i < sv.getIndices().length; i++) {
                    for (int k = 0; k < numTopic; k++) {
                        wordTopicStat.add(k, sv.getIndices()[i], topicDistributionTuple.f1.get(k, i));
                    }
                }
                gammad = topicDistributionTuple.f0;
                DenseMatrix deGammad = LdaUtil.dirichletExpectationVec(topicDistributionTuple.f0);
                for (int k = 0; k < numTopic; k++) {
                    logPhatPart.add(k, 0, deGammad.get(k, 0));
                }
                nonEmptyDocCount++;
            }
        }
        return new Tuple4<>(wordTopicStat, logPhatPart, nonEmptyWordCount, nonEmptyDocCount);
    }
}
