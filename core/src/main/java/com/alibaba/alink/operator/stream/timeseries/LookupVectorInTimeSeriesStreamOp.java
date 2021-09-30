package com.alibaba.alink.operator.stream.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.timeseries.LookupVectorInTimeSeriesMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.timeseries.LookupVectorInTimeSeriesParams;

public class LookupVectorInTimeSeriesStreamOp extends MapStreamOp <LookupVectorInTimeSeriesStreamOp>
	implements LookupVectorInTimeSeriesParams <LookupVectorInTimeSeriesStreamOp> {

	public LookupVectorInTimeSeriesStreamOp() {
		this(null);
	}

	public LookupVectorInTimeSeriesStreamOp(Params params) {
		super(LookupVectorInTimeSeriesMapper::new, params);
	}
}
