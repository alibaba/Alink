package com.alibaba.alink.operator.common.clustering;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.params.clustering.LdaTrainParams;

import java.io.Serializable;
import java.util.List;

/**
 * The model data of lda algorithm.
 */
public class LdaModelData implements Serializable {
	private static final long serialVersionUID = -6704560030183165653L;

	public LdaModelData() {}

	public LdaModelData(int topicNum, int vocabularySize, DenseMatrix gamma, double[] alpha, double[] beta) {
		this.topicNum = topicNum;
		this.vocabularySize = vocabularySize;
		this.gamma = gamma;
		this.alpha = alpha;
		this.beta = beta;
	}

	public LdaModelData(int topicNum, int vocabularySize, double[] alpha, double[] beta, DenseMatrix wordTopicCounts) {
		this.topicNum = topicNum;
		this.vocabularySize = vocabularySize;
		this.alpha = alpha;
		this.beta = beta;
		this.wordTopicCounts = wordTopicCounts;
	}

	//wordTopicCounts and topicCounts
	public DenseMatrix gamma;
	public DenseMatrix wordTopicCounts;
	public double[] alpha;
	public double[] beta;
	public int topicNum;
	public int vocabularySize;
	//the optimizer strategy.
	public LdaTrainParams.Method optimizer = LdaTrainParams.Method.EM;
	//for calculate perplexity.
	public double logPerplexity;
	public double logLikelihood;

	//doc count data.
	public List <String> list;

	public Integer seed;
}
