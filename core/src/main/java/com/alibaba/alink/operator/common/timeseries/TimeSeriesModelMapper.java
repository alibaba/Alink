package com.alibaba.alink.operator.common.timeseries;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.common.timeseries.TimeSeriesMapper.Predictor;
import com.alibaba.alink.operator.common.timeseries.TimeSeriesMapper.TimeSeries;

import java.sql.Timestamp;

public abstract class TimeSeriesModelMapper extends ModelMapper {
	private TimeSeries <TimeSeriesModelMapper> timeSeries;

	public TimeSeriesModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	public void open() {
		this.timeSeries = new TimeSeries <>(
			params,
			new Predictor <>(
				this,
				TimeSeriesModelMapper::predictSingleVar,
				TimeSeriesModelMapper::predictMultiVar
			)
		);
	}

	@Override
	protected final void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		timeSeries.map(selection, result);
	}

	protected abstract Tuple2 <double[], String> predictSingleVar(
		Timestamp[] historyTimes, double[] historyVals, int predictNum);

	protected abstract Tuple2 <Vector[], String> predictMultiVar(
		Timestamp[] historyTimes, Vector[] historyVals, int predictNum);

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema modelSchema, TableSchema dataSchema, Params params) {

		return TimeSeriesMapper.prepareTimeSeriesIoSchema(params);

	}
}