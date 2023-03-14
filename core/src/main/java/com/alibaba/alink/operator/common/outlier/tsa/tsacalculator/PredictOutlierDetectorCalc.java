package com.alibaba.alink.operator.common.outlier.tsa.tsacalculator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.params.outlier.tsa.HasTrainNum;

import java.io.Serializable;

public abstract class PredictOutlierDetectorCalc implements Serializable {
	private static final long serialVersionUID = 8451571015820252352L;
	public int trainNum;

	public PredictOutlierDetectorCalc() {}
	public PredictOutlierDetectorCalc(Params params) {
		this.trainNum = params.get(HasTrainNum.TRAIN_NUM);
	}

	//this is used in fitted data detect, and the temp sum and square sum will be saved to speed up calculation.
	//return whether the last data is outlier or not. If outlier, return the modified data.
	// todo only used in test.
	public abstract Tuple2 <Boolean, Double> predictBatchLast(double[] data);

	public void setTrainNum(int trainNum) {
		this.trainNum = trainNum;
	}

	/**
	 * @param realData   the real data.
	 * @param formerData the former residual data which is used to help detect outliers. In this function, it will be
	 *                   updated with current residual data and help in the next batch data.
	 * @param predData   the predicted data.
	 */
	public abstract int[] detectAndUpdateFormerData(double[] realData, double[] formerData, double[] predData);

	public abstract SparseVector detect(double[] data);

	public abstract void trainModel(double[] data);

	public abstract PredictOutlierDetectorCalc clone();

	public abstract void reset();
}
