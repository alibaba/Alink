package com.alibaba.alink.operator.common.classification;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.params.classification.NaiveBayesTextTrainParams.ModelType;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * The train model of naive bayes.
 */
public class NaiveBayesTextTrainModelData {

	//	public Params meta = new Params();
	public ModelType modelType;
	public String vectorColName;
	public DenseMatrix theta;
	public double[] pi;
	public Object[] label;
	public ArrayList<Tuple3<Object, Double, DenseVector>> modelArray;
	public int vectorSize;
	public String[] featureColNames = null;
}
