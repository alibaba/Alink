package com.alibaba.alink.operator.common.clustering.lda;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;

import java.util.List;

/**
 * online loglikelihood.
 */
public class OnlineLogLikelihood extends ComputeFunction {

	private double beta;
	private int numTopic;
	private int numIter;

	/**
	 * Constructor.
	 */
	public OnlineLogLikelihood(double beta, int numTopic, int numIter) {
		this.beta = beta;
		this.numTopic = numTopic;
		this.numIter = numIter;
	}

	/**
	 * Calculate the log likelihood.
	 */
	public static double logLikelihood(List <Vector> data, DenseMatrix lambda, DenseMatrix alpha, DenseMatrix gammad,
									   int numTopic, int vocabularySize, double beta, int taskNum) {

		boolean isRandGamma = gammad == null;

		DenseMatrix ELogBeta = LdaUtil.dirichletExpectation(lambda).transpose();
		DenseMatrix expELogBeta = LdaUtil.expDirichletExpectation(lambda).transpose();

		double corpusPart = 0;
		//corpus part: E[log p(theta | alpha) - log q(theta | gamma)]
		if (data != null) {
			for (Vector vector : data) {
				double docBound = 0;
				SparseVector sv = (SparseVector) vector;
				sv.removeZeroValues();

				if (isRandGamma) {
					gammad = LdaUtil.geneGamma(numTopic);
				}
				gammad = LdaUtil.getTopicDistributionMethod(sv, expELogBeta, alpha, gammad, numTopic).f0;
				DenseMatrix ELogThetad = LdaUtil.dirichletExpectationVec(gammad);

				for (int i = 0; i < sv.numberOfValues(); i++) {
					DenseMatrix ELogBetaDoc = new DenseMatrix(numTopic, 1, ELogBeta.getRow(sv.getIndices()[i]));
					docBound += sv.getValues()[i] * logSumExp(ELogThetad.plus(ELogBetaDoc));
				}

				docBound += LdaUtil.elementWiseProduct(alpha.minus(gammad), ELogThetad).sum();
				docBound += LdaUtil.lgamma(gammad).minus(LdaUtil.lgamma(alpha)).sum();
				docBound += LdaUtil.lgamma(alpha.sum()) - LdaUtil.lgamma(gammad.sum());

				corpusPart += docBound;
			}
		}

		double sumEta = beta * vocabularySize;

		double topicsPart = LdaUtil.elementWiseProduct(lambda.transpose().plus(-beta).scale(-1), ELogBeta)
			.sum()
			+
			LdaUtil.lgamma(lambda.transpose()).plus(-LdaUtil.lgamma(beta)).sum() -
			LdaUtil.lgamma(LdaUtil.sumByRow(lambda.transpose())).plus(-LdaUtil.lgamma(sumEta)).sum();

		double logLikelihood = corpusPart + topicsPart / taskNum;

		return logLikelihood;
	}

	public static double logSumExp(DenseMatrix dm) {
		double max = dm.get(0, 0);
		for (int i = 0; i < dm.numRows(); i++) {
			for (int j = 0; j < dm.numCols(); j++) {
				if (dm.get(i, j) > max) {
					max = dm.get(i, j);
				}
			}
		}
		double sum = 0;
		for (int i = 0; i < dm.numRows(); i++) {
			for (int j = 0; j < dm.numCols(); j++) {
				sum += Math.exp(dm.get(i, j) - max);
			}
		}
		return max + Math.log(sum);
	}

	@Override
	public void calc(ComContext context) {

		double[] logLikeliHooks = new double[]{0};
		context.putObj(LdaVariable.logLikelihood, logLikeliHooks);
		if (context.getStepNo() == numIter) {
			Tuple2<Long, Integer> tuple2 = ((List<Tuple2<Long, Integer>>) context.getObj(LdaVariable.shape)).get(0);
			int vocabularySize = tuple2.f1;

			DenseMatrix lambda = context.getObj(LdaVariable.lambda);
			DenseMatrix alpha = context.getObj(LdaVariable.alpha);

			//get data
			List<Vector> data = context.getObj(LdaVariable.data);

			int taskNum = context.getNumTask();

			DenseMatrix gammad = null;

			double logLikelihood = logLikelihood(data, lambda, alpha, gammad,
				numTopic, vocabularySize, beta, taskNum);

			logLikeliHooks[0] = logLikelihood;
			context.putObj(LdaVariable.logLikelihood, logLikeliHooks);
		}
	}
}
