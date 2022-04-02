package com.alibaba.alink.operator.stream.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.timeseries.LookupVectorInTimeSeriesMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.timeseries.LookupVectorInTimeSeriesParams;

@ParamSelectColumnSpec(name="timeCol",allowedTypeCollections = TypeCollections.TIMESTAMP_TYPES)
@ParamSelectColumnSpec(name="timeSeriesCol",allowedTypeCollections = TypeCollections.MTABLE_TYPES)
@NameCn("时间序列向量插值")
public class LookupVectorInTimeSeriesStreamOp extends MapStreamOp <LookupVectorInTimeSeriesStreamOp>
	implements LookupVectorInTimeSeriesParams <LookupVectorInTimeSeriesStreamOp> {

	public LookupVectorInTimeSeriesStreamOp() {
		this(null);
	}

	public LookupVectorInTimeSeriesStreamOp(Params params) {
		super(LookupVectorInTimeSeriesMapper::new, params);
	}
}
