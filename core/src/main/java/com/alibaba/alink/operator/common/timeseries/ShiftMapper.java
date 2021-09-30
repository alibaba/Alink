package com.alibaba.alink.operator.common.timeseries;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.params.timeseries.ShiftParams;

import java.sql.Timestamp;

public class ShiftMapper extends TimeSeriesSingleMapper {
	private int shiftNum;

	public ShiftMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.shiftNum = params.get(ShiftParams.SHIFT_NUM);
	}

	@Override
	protected Tuple2 <double[], String> predictSingleVar(Timestamp[] historyTimes,
														 double[] historyVals,
														 int predictNum) {
		if (historyVals == null || historyVals.length == 0) {
			return Tuple2.of(null, null);
		}

		double[] result = new double[predictNum];
		int n = historyVals.length;

		int shiftNumLocal = shiftNum > n ? n : shiftNum;

		for (int i = 0; i < predictNum; i++) {
			result[i] = historyVals[n - shiftNumLocal + i % shiftNumLocal];
		}

		return Tuple2.of(result, null);
	}

}
