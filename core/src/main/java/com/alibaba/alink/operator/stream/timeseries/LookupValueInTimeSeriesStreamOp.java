package com.alibaba.alink.operator.stream.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.timeseries.LookupValueInTimeSeriesMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.timeseries.LookupValueInTimeSeriesParams;

@ParamSelectColumnSpec(name="timeCol",allowedTypeCollections = TypeCollections.TIMESTAMP_TYPES)
@ParamSelectColumnSpec(name="timeSeriesCol",allowedTypeCollections = TypeCollections.MTABLE_TYPES)
@NameCn("时间序列插值")
public class LookupValueInTimeSeriesStreamOp extends MapStreamOp <LookupValueInTimeSeriesStreamOp>
	implements LookupValueInTimeSeriesParams <LookupValueInTimeSeriesStreamOp> {

	public LookupValueInTimeSeriesStreamOp() {
		this(null);
	}

	public LookupValueInTimeSeriesStreamOp(Params params) {
		super(LookupValueInTimeSeriesMapper::new, params);
	}
}
