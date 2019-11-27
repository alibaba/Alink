package com.alibaba.alink.operator.common.classification;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.operator.common.classification.NaiveBayesModelDataConverter.BayesType;

/**
 * The train model of naive bayes.
 */
public class NaiveBayesTrainModelData {

//	public Params meta = new Params();
	public BayesType modelType;
	public String[] featureNames;
	public DenseMatrix theta;
	public double[] pi;
	public Object[] label;
}
