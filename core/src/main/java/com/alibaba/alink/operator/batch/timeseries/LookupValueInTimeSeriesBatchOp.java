package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.timeseries.LookupValueInTimeSeriesMapper;
import com.alibaba.alink.params.timeseries.LookupValueInTimeSeriesParams;

@ParamSelectColumnSpec(name="timeCol",allowedTypeCollections = TypeCollections.TIMESTAMP_TYPES)
@ParamSelectColumnSpec(name="timeSeriesCol",allowedTypeCollections = TypeCollections.MTABLE_TYPES)
@NameCn("时间序列插值")
@NameEn("Lookup Value In Time Series")
public class LookupValueInTimeSeriesBatchOp extends MapBatchOp <LookupValueInTimeSeriesBatchOp>
	implements LookupValueInTimeSeriesParams <LookupValueInTimeSeriesBatchOp> {

	public LookupValueInTimeSeriesBatchOp() {
		this(null);
	}

	public LookupValueInTimeSeriesBatchOp(Params params) {
		super(LookupValueInTimeSeriesMapper::new, params);
	}
}
