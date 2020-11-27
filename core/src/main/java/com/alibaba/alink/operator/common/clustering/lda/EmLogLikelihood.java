package com.alibaba.alink.operator.common.clustering.lda;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.linalg.BLAS;
import com.alibaba.alink.common.linalg.DenseMatrix;

import java.util.List;

/**
 * Calculate em log likelihood.
 */
public class EmLogLikelihood extends ComputeFunction {

	private static final long serialVersionUID = 5852918530626254844L;
	private int numTopic;
	private double alpha;
	private double beta;
	private int numIter;

	/**
	 * Constructor.
	 *
	 * @param numTopic the number of topics.
	 * @param alpha    alpha param.
	 * @param beta     beta param.
	 * @param numIter  the number of iterations.
	 */
	public EmLogLikelihood(int numTopic, double alpha, double beta, int numIter) {
		this.numTopic = numTopic;
		this.alpha = alpha;
		this.beta = beta;
		this.numIter = numIter;
	}

	@Override
	public void calc(ComContext context) {
		int stepNo = context.getStepNo();
		if (stepNo == 1) {
			double[] logLikelihoodHook = new double[1];
			context.putObj(LdaVariable.logLikelihood, logLikelihoodHook);
		}
		if (stepNo == this.numIter) {
			double[] logLikelihoodHook = context.getObj(LdaVariable.logLikelihood);
			Document[] docs = context.getObj(LdaVariable.corpus);
			if (docs == null) {
				return;
			}
			int vocabularySize = ((List <Integer>) context.getObj(LdaVariable.vocabularySize)).get(0);
			DenseMatrix nDocTopics = context.getObj(LdaVariable.nDocTopics);
			DenseMatrix nWordTopics = new DenseMatrix(vocabularySize + 1, numTopic,
				context.getObj(LdaVariable.nWordTopics), false);
			double logLikelihood = 0;
			double[] probTopicGivenDoc = new double[numTopic];
			double[] probWordGivenTopic = new double[numTopic];
			for (int i = 0; i < docs.length; i++) {
				Document doc = docs[i];
				int length = doc.getLength();
				for (int k = 0; k < numTopic; k++) {
					//the probability that doc i belongs to topic k.
					probTopicGivenDoc[k] = (nDocTopics.get(i, k) + alpha) / (length + alpha * numTopic);
				}
				for (int j = 0; j < length; j++) {
					int wordId = doc.getWordIdxs(j);
					for (int k = 0; k < numTopic; k++) {
						//the probability that word i of a certain doc belongs to k
						probWordGivenTopic[k] = (nWordTopics.get(wordId, k) + beta) / (nWordTopics.get(vocabularySize,
							k)
							+ beta * vocabularySize);
					}
					double wSum = BLAS.dot(probTopicGivenDoc, probWordGivenTopic);
					logLikelihood += Math.log(wSum);
				}
			}
			logLikelihoodHook[0] = logLikelihood;
			context.putObj(LdaVariable.logLikelihood, logLikelihoodHook);
		}
	}
}
