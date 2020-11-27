package com.alibaba.alink.operator.common.classification;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.SparseVector;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

public class NaiveBayesModelData implements Serializable {
	private static final long serialVersionUID = 3919917903722286395L;
	public String[] featureNames;
	public Number[][][] theta;
	public double[] piArray;
	public double[] labelWeights;
	public Object[] label;
	public boolean[] isCate;
	public List <Row> stringIndexerModelSerialized;

	public double[][] weightSum;
	public SparseVector[][] featureInfo;

	public void generateWeightAndNumbers(List <Tuple3 <Object, Double[], HashMap <Integer, Double>[]>> arrayData) {
		int arrayLength = arrayData.size();
		int featureNumber = arrayData.get(0).f1.length;
		weightSum = new double[arrayLength][featureNumber];
		featureInfo = new SparseVector[arrayLength][featureNumber];
		for (int i = 0; i < arrayLength; i++) {
			weightSum[i] = ArrayUtils.toPrimitive(arrayData.get(i).f1);
			HashMap <Integer, Double>[] numbersPerLabel = arrayData.get(i).f2;
			featureInfo[i] = map2sv(numbersPerLabel, featureNumber);
		}
	}

	private static SparseVector[] map2sv(HashMap <Integer, Double>[] map, int featureNumber) {
		SparseVector[] res = new SparseVector[featureNumber];
		for (int i = 0; i < featureNumber; i++) {
			res[i] = new SparseVector(-1, map[i]);
		}
		return res;
	}
}
