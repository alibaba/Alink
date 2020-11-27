package com.alibaba.alink.operator.common.clustering.lda;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.List;

/**
 * online loglikelihood.
 */
public class OnlineLogLikelihood extends ComputeFunction {

	private static final long serialVersionUID = -675551201422550951L;
	private double beta;
	private int numTopic;
	private int numIter;
	private int gammaShape;
	private Integer seed;
	private RandomDataGenerator random = new RandomDataGenerator();
	private boolean addedIndex = false;

	/**
	 * Constructor.
	 */
	public OnlineLogLikelihood(double beta, int numTopic, int numIter, int gammaShape, Integer seed) {
		this.beta = beta;
		this.numTopic = numTopic;
		this.numIter = numIter;
		this.gammaShape = gammaShape;
		this.seed = seed;
	}

	/**
	 * Calculate the log likelihood.
	 */
	public static double logLikelihood(List <Vector> data, DenseMatrix lambda, DenseMatrix alpha, DenseMatrix gammad,
									   int numTopic, int vocabularySize,
									   double beta, int taskNum, int gammaShape, RandomDataGenerator random) {

		boolean isRandGamma = gammad == null;

		DenseMatrix ELogBeta = LdaUtil.dirichletExpectation(lambda).transpose();
		DenseMatrix expELogBeta = LdaUtil.expDirichletExpectation(lambda).transpose();

		double corpusPart = 0;
		//corpus part: E[log p(theta | alpha) - log q(theta | Gamma)]
		if (data != null) {
			for (Vector vector : data) {
				double docBound = 0;
				SparseVector sv = (SparseVector) vector;
				sv.removeZeroValues();

				if (isRandGamma) {
					gammad = LdaUtil.geneGamma(numTopic, gammaShape, random);
				}
				gammad = LdaUtil.getTopicDistributionMethod(sv, expELogBeta, alpha, gammad, numTopic).f0;
				DenseMatrix ELogThetad = LdaUtil.dirichletExpectationVec(gammad);

				for (int i = 0; i < sv.numberOfValues(); i++) {
					DenseMatrix ELogBetaDoc = new DenseMatrix(numTopic, 1, ELogBeta.getRow(sv.getIndices()[i]));
					docBound += sv.getValues()[i] * LdaUtil.logSumExp(ELogThetad.plus(ELogBetaDoc));
				}

				docBound += LdaUtil.elementWiseProduct(alpha.minus(gammad), ELogThetad).sum();
				docBound += LdaUtil.logGamma(gammad).minus(LdaUtil.logGamma(alpha)).sum();
				docBound += LdaUtil.logGamma(alpha.sum()) - LdaUtil.logGamma(gammad.sum());

				corpusPart += docBound;
			}

		}

		double sumEta = beta * vocabularySize;

		double topicsPart = LdaUtil.elementWiseProduct(lambda.transpose().plus(-beta).scale(-1), ELogBeta).sum() +
			LdaUtil.logGamma(lambda.transpose()).plus(-LdaUtil.logGamma(beta)).sum() -
			LdaUtil.logGamma(LdaUtil.sumByRow(lambda.transpose())).plus(-LdaUtil.logGamma(sumEta)).sum();

		double logLikelihood = corpusPart + topicsPart / taskNum;

		return logLikelihood;
	}

	//todo just calculate log likelihood in the last inter.
	@Override
	public void calc(ComContext context) {
		int stepNo = context.getStepNo();
		if (!addedIndex && seed != null) {
			random.reSeed(seed);
			addedIndex = true;
		}
		if (stepNo == 1) {
			double[] logLikelihoodHook = new double[1];
			context.putObj(LdaVariable.logLikelihood, logLikelihoodHook);
		}
		//only calculate log likelihood in the last iter.
		if (stepNo == this.numIter) {
			double[] logLikelihoodHook = context.getObj(LdaVariable.logLikelihood);

			Tuple2 <Long, Integer> tuple2 = ((List <Tuple2 <Long, Integer>>) context.getObj(LdaVariable.shape)).get(0);
			int vocabularySize = tuple2.f1;

			DenseMatrix lambda = context.getObj(LdaVariable.lambda);
			DenseMatrix alpha = context.getObj(LdaVariable.alpha);

			//get data
			List <Vector> data = context.getObj(LdaVariable.data);

			int taskNum = context.getNumTask();

			DenseMatrix gammad = null;
			double logLikelihood = logLikelihood(data, lambda, alpha, gammad,
				numTopic, vocabularySize, beta, taskNum, gammaShape, random);

			logLikelihoodHook[0] = logLikelihood;
			context.putObj(LdaVariable.logLikelihood, logLikelihoodHook);
		}
	}
}
