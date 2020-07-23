package com.alibaba.alink.operator.common.classification;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.params.classification.NaiveBayesTextTrainParams.ModelType;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.DenseMatrix;

import java.util.ArrayList;

/**
 * The predict model of naive bayes.
 */
public class NaiveBayesTextPredictModelData {
	public Params meta = new Params();
	public ModelType modelType;
	public String vectorColName;
	public String[] featureCols;
	/**
	 * the label of the naive bayes model.
	 */
	public Object[] label;
	/**
	 * the priori probability of each label.
	 */
	public double[] pi;
	/**
	 * the conditional probability of label.
	 */
	public DenseMatrix theta;
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
	/**
	 * this is model array for the modelInfo.
	 */
	public ArrayList<Tuple3<Object, Double, DenseVector>> modelArray;
	/**
	 * the vector size of train data.
	 */
	public int vectorSize;
}