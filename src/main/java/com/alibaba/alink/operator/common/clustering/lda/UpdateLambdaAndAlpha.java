package com.alibaba.alink.operator.common.clustering.lda;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.linalg.DenseMatrix;

import java.util.List;

/**
 * Update lambda and alpha.
 */
public class UpdateLambdaAndAlpha extends ComputeFunction {
    private int numTopic;

    private double tau0;
    private double kappa;
    private double eta;
    private double subSampleRatio;

    private boolean optimizeDocConcentration;

    /**
     * Constructor.
     */
    public UpdateLambdaAndAlpha(int numTopic, double learningOffset, double learningRate,
                                double subSampleRatio, boolean optimizeDocConcentration, double eta) {

        this.numTopic = numTopic;
        this.tau0 = learningOffset;
        this.kappa = learningRate;
        this.eta = eta;
        this.subSampleRatio = subSampleRatio;
        this.optimizeDocConcentration = optimizeDocConcentration;
    }

    /**
     * Update the lambda and alpha parameter. The first of the return data is lambda, and the second one is alpha.
     */
    static Tuple2<DenseMatrix, DenseMatrix> calculateLambdaAndAlpha(
            DenseMatrix lambda, DenseMatrix alpha, DenseMatrix wordTopicStat, DenseMatrix logPhat,
            long nonEmptyDocsN, int iterNum, double tau0, double kappa, double eta,
            double subSampleRatio, int numTopic, boolean optimizeDocConcentration) {
        DenseMatrix expELogBeta = LdaUtil.expDirichletExpectation(lambda).transpose();
        //the weight given to lambda.
        double weight = Math.pow(tau0 + iterNum, -kappa);
        DenseMatrix batchResult = LdaUtil.elementWiseProduct(wordTopicStat, expELogBeta.transpose());
        //update lambda.
        lambda = lambda.scale(1 - weight)
                .plus(batchResult.scale(1.0 / subSampleRatio).plus(eta).scale(weight));
        if (optimizeDocConcentration) {
            logPhat.scaleEqual(1.0 / nonEmptyDocsN);
            DenseMatrix gradf = LdaUtil.dirichletExpectationVec(alpha).minus(logPhat).scale(-1.0 * nonEmptyDocsN);
            double c = nonEmptyDocsN * LdaUtil.trigamma(alpha.sum());
            DenseMatrix q = LdaUtil.trigamma(alpha).scale(-1.0 * nonEmptyDocsN);
            DenseMatrix reciprocalQ = LdaUtil.elementWiseDivide(DenseMatrix.ones(numTopic, 1), q);
            double b = LdaUtil.elementWiseDivide(gradf, q).sum() / (1.0 / c + reciprocalQ.sum());
            DenseMatrix dAlpha = LdaUtil.elementWiseDivide(gradf.plus(-b), q).scale(-1.0);
            DenseMatrix tmpAlpha = dAlpha.scale(weight).plus(alpha);
            for (int i = 0; i < numTopic; i++) {
                if (tmpAlpha.get(i, 0) <= 0) {
                    return new Tuple2<>(lambda, alpha);
                }
            }
            return new Tuple2<>(lambda, tmpAlpha);
        } else {
            return new Tuple2<>(lambda, alpha);
        }
    }

    @Override
    public void calc(ComContext context) {
        int iterNum = context.getStepNo();
        Tuple2<Long, Integer> tuple2 = ((List<Tuple2<Long, Integer>>) context.getObj(LdaVariable.shape)).get(0);
        int vocabularySize = tuple2.f1;
        DenseMatrix lambda;
        DenseMatrix alpha;
        if (context.getStepNo() == 1) {
            Tuple2<DenseMatrix, DenseMatrix> initGammaAndAlpha =
                    ((List<Tuple2<DenseMatrix, DenseMatrix>>) context.getObj(LdaVariable.initModel)).get(0);
            lambda = initGammaAndAlpha.f0;
            alpha = initGammaAndAlpha.f1;
        } else {
            lambda = context.getObj(LdaVariable.lambda);
            alpha = context.getObj(LdaVariable.alpha);
        }
        DenseMatrix wordTopicStat = new DenseMatrix(numTopic, vocabularySize, context.getObj(LdaVariable.wordTopicStat));
        DenseMatrix logPhat = new DenseMatrix(numTopic, 1, context.getObj(LdaVariable.logPhatPart));
        long nonEmptyDocsN = Math.round(((double[]) context.getObj(LdaVariable.nonEmptyDocCount))[0]);
        Tuple2<DenseMatrix, DenseMatrix> out = calculateLambdaAndAlpha(lambda, alpha, wordTopicStat, logPhat,
                nonEmptyDocsN, iterNum, tau0, kappa, eta, subSampleRatio,
                numTopic, optimizeDocConcentration);
        context.putObj(LdaVariable.lambda, out.f0);
        context.putObj(LdaVariable.alpha, out.f1);
    }
}
