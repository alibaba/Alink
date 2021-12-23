package com.alibaba.alink.operator.stream.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.timeseries.LookupValueInTimeSeriesMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.timeseries.LookupValueInTimeSeriesParams;

public class LookupValueInTimeSeriesStreamOp extends MapStreamOp <LookupValueInTimeSeriesStreamOp>
	implements LookupValueInTimeSeriesParams <LookupValueInTimeSeriesStreamOp> {

	public LookupValueInTimeSeriesStreamOp() {
		this(null);
	}

	public LookupValueInTimeSeriesStreamOp(Params params) {
		super(LookupValueInTimeSeriesMapper::new, params);
	}
}
