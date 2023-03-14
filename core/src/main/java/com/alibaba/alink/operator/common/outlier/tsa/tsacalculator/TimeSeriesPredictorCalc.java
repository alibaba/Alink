package com.alibaba.alink.operator.common.outlier.tsa.tsacalculator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import java.io.Serializable;
import java.util.Arrays;

public abstract class TimeSeriesPredictorCalc implements Serializable {

	private static final long serialVersionUID = 7329549906350002422L;
	public Integer predictNum;
	public int groupNumber;

	public void setGroupNumber(int groupNumber) {
		this.groupNumber = groupNumber;
	}

	// todo: for the train of time series predict algo may fail, we catch the exception and
	//  fill all the predicted data with mean of train data.
	public Tuple2<double[], Boolean> forecast(double[] data, int forecastStep, boolean trainBeforeForecast) {
		try {
			return Tuple2.of(forecastWithoutException(data, forecastStep, trainBeforeForecast), false);
		}  catch (Exception e) {
			double[] res = new double[forecastStep];
			double sum = 0;
			int count = 0;
			for (double datum : data) {
				++count;
				sum += datum;
			}
			sum /= count;//mean
			Arrays.fill(res, sum);
			return Tuple2.of(res, true);
		}
	}

	public abstract double[] forecastWithoutException(double[] data, int forecastStep, boolean trainBeforeForecast);

	public abstract double[] predict(double[] data);

	//this helps map. If output has more column than just predict, than use this.
	public abstract Row map(Row in);

	public void reset() {}

	public Row getData(Row group, Object... others) {
		return getData(group, groupNumber, others);
	}


	public abstract TimeSeriesPredictorCalc clone();

	public String getCurrentModel() {
		return null;
	}

	public void setCurrentModel(String currentModel) {

	}

	public static Row getGroupData(Row group, int groupNumber, Object... others) {
		int othersLength = others.length;
		Row res = new Row(groupNumber + othersLength);
		for (int i = 0; i < groupNumber; i++) {
			res.setField(i, group.getField(i));
		}
		for (int i = 0; i < othersLength; i++) {
			res.setField(groupNumber + i, others[i]);
		}
		return res;
	}
}
