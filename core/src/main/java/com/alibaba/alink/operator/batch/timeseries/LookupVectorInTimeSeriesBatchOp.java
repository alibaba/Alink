package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.timeseries.LookupVectorInTimeSeriesMapper;
import com.alibaba.alink.params.timeseries.LookupVectorInTimeSeriesParams;

public class LookupVectorInTimeSeriesBatchOp extends MapBatchOp <LookupVectorInTimeSeriesBatchOp>
	implements LookupVectorInTimeSeriesParams<LookupVectorInTimeSeriesBatchOp> {

	public LookupVectorInTimeSeriesBatchOp() {
		this(null);
	}

	public LookupVectorInTimeSeriesBatchOp(Params params) {
		super(LookupVectorInTimeSeriesMapper::new, params);
	}
}
