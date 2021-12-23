package com.alibaba.alink.operator.common.timeseries;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.utils.JsonConverter;

import java.sql.Timestamp;

public abstract class TimeSeriesSingleMapper extends TimeSeriesMapper {

	TimeSeriesSingleMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	@Override
	protected Tuple2 <Vector[], String> predictMultiVar(
		Timestamp[] historyTimes, Vector[] historyVals, int predictNum) {

		int vectorSize = historyVals[0].size();
		int trainNum = historyVals.length;
		double[] historySingleVals = new double[trainNum];
		Vector[] results = new Vector[predictNum];
		for (int i = 0; i < results.length; i++) {
			results[i] = new DenseVector(vectorSize);
		}
		String[] infos = new String[vectorSize];
		for (int i = 0; i < vectorSize; i++) {
			for (int j = 0; j < trainNum; j++) {
				historySingleVals[j] = historyVals[j].get(i);
			}
			Tuple2 <double[], String> out = predictSingleVar(historyTimes, historySingleVals, predictNum);

			if (out.f0 != null) {
				for (int j = 0; j < predictNum; j++) {
					results[j].set(i, out.f0[j]);
				}
			}
			if (out.f1 != null) {
				infos[i] = out.f1;
			}
		}
		return Tuple2.of(results, getPredictionDetails(infos));
	}

	protected String getPredictionDetails(String[] singelPredictionDetails) {
		if(singelPredictionDetails == null) {
			return null;
		}
		return JsonConverter.toJson(singelPredictionDetails);
	}

}