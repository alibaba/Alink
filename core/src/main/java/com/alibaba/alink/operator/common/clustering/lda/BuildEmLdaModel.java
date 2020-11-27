package com.alibaba.alink.operator.common.clustering.lda;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompleteResultFunction;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.operator.common.clustering.LdaModelData;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Build em lda model.
 */
public class BuildEmLdaModel extends CompleteResultFunction {
	private static final Logger LOG = LoggerFactory.getLogger(BuildEmLdaModel.class);
	private static final long serialVersionUID = 8585414464252665425L;
	private int topicNum;
	private double alpha;
	private double beta;

	/**
	 * Constructor.
	 *
	 * @param topicNum the number of topics.
	 * @param alpha    the alpha param.
	 * @param beta     the beta param.
	 */
	public BuildEmLdaModel(int topicNum, double alpha, double beta) {
		this.topicNum = topicNum;
		this.alpha = alpha;
		this.beta = beta;
	}

	@Override
	public List <Row> calc(ComContext context) {
		if (context.getTaskId() != 0) {
			return null;
		}

		double[] logLikelihoods = context.getObj(LdaVariable.logLikelihood);
		LOG.info("em logLikelihood: {}", logLikelihoods[0]);
		int vocabularySize = ((List <Integer>) context.getObj(LdaVariable.vocabularySize)).get(0);
		DenseMatrix gamma = new DenseMatrix(vocabularySize + 1, topicNum,
			context.getObj(LdaVariable.nWordTopics), false);
		double[] alphaVec = new double[topicNum];
		Arrays.fill(alphaVec, alpha);
		double[] betaVec = new double[topicNum];
		Arrays.fill(betaVec, beta);
		LdaModelData modelData = new LdaModelData(topicNum, vocabularySize, gamma, alphaVec, betaVec);

		modelData.logLikelihood = logLikelihoods[0];
		modelData.logPerplexity = -modelData.logLikelihood / vocabularySize;
		return Lists.newArrayList(Row.of(modelData));
	}
}
