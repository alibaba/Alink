package com.alibaba.alink.operator.local.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.timeseries.LookupValueInTimeSeriesMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.timeseries.LookupValueInTimeSeriesParams;

@ParamSelectColumnSpec(name = "timeCol", allowedTypeCollections = TypeCollections.TIMESTAMP_TYPES)
@ParamSelectColumnSpec(name = "timeSeriesCol", allowedTypeCollections = TypeCollections.MTABLE_TYPES)
@NameCn("时间序列插值")
public class LookupValueInTimeSeriesLocalOp extends MapLocalOp <LookupValueInTimeSeriesLocalOp>
	implements LookupValueInTimeSeriesParams <LookupValueInTimeSeriesLocalOp> {

	public LookupValueInTimeSeriesLocalOp() {
		this(null);
	}

	public LookupValueInTimeSeriesLocalOp(Params params) {
		super(LookupValueInTimeSeriesMapper::new, params);
	}
}
