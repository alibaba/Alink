package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.timeseries.LookupValueInTimeSeriesMapper;
import com.alibaba.alink.params.timeseries.LookupValueInTimeSeriesParams;

public class LookupValueInTimeSeriesBatchOp extends MapBatchOp <LookupValueInTimeSeriesBatchOp>
	implements LookupValueInTimeSeriesParams <LookupValueInTimeSeriesBatchOp> {

	public LookupValueInTimeSeriesBatchOp() {
		this(null);
	}

	public LookupValueInTimeSeriesBatchOp(Params params) {
		super(LookupValueInTimeSeriesMapper::new, params);
	}
}
