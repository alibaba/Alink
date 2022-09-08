package com.alibaba.alink.operator.local.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.timeseries.LookupVectorInTimeSeriesMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.timeseries.LookupVectorInTimeSeriesParams;

@ParamSelectColumnSpec(name = "timeCol", allowedTypeCollections = TypeCollections.TIMESTAMP_TYPES)
@ParamSelectColumnSpec(name = "timeSeriesCol", allowedTypeCollections = TypeCollections.MTABLE_TYPES)
@NameCn("时间序列向量插值")
public class LookupVectorInTimeSeriesLocalOp extends MapLocalOp <LookupVectorInTimeSeriesLocalOp>
	implements LookupVectorInTimeSeriesParams <LookupVectorInTimeSeriesLocalOp> {

	public LookupVectorInTimeSeriesLocalOp() {
		this(null);
	}

	public LookupVectorInTimeSeriesLocalOp(Params params) {
		super(LookupVectorInTimeSeriesMapper::new, params);
	}
}
