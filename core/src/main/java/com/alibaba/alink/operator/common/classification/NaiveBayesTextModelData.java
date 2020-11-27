package com.alibaba.alink.operator.common.classification;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.params.classification.NaiveBayesTextTrainParams.ModelType;

/**
 * The predict model of naive bayes.
 */
public class NaiveBayesTextModelData {
	public ModelType modelType;
	public String vectorColName;
	public String[] featureCols = null;

	/**
	 * the vector size of train data.
	 */
	public int vectorSize;

	/**
	 * the label of the naive bayes model.
	 */
	public Object[] labels;

	public double[] pi;

	public double[] phi;

	public DenseMatrix theta;

	public String[] featureColNames;

	public DenseMatrix minMat;
}