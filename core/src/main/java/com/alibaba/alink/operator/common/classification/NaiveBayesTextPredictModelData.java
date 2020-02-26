package com.alibaba.alink.operator.common.classification;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.operator.common.classification.NaiveBayesTextModelDataConverter.BayesType;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * The predict model of naive bayes.
 */
public class NaiveBayesTextPredictModelData {
	protected Params meta = new Params();
	protected BayesType modelType;
	protected String vectorColName;
	/**
	 * the label of the naive bayes model.
	 */
	protected Object[] label;
	/**
	 * the priori probability of each label.
	 */
	protected double[] pi;
	/**
	 * the conditional probability of label.
	 */
	protected DenseMatrix theta;
	/**
	 * the feature length.
	 */
	protected int featLen;
	/**
	 * the conditional probability for bernoulli type.
	 */
	protected DenseMatrix minMat;
	/**
	 * the probability params for bernoulli type.
	 */
	protected double[] phi;

}