package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.timeseries.LookupVectorInTimeSeriesMapper;
import com.alibaba.alink.params.timeseries.LookupVectorInTimeSeriesParams;

@ParamSelectColumnSpec(name="timeCol",allowedTypeCollections = TypeCollections.TIMESTAMP_TYPES)
@ParamSelectColumnSpec(name="timeSeriesCol",allowedTypeCollections = TypeCollections.MTABLE_TYPES)
@NameCn("时间序列向量插值")
@NameEn("Lookup Vector In Time Series")
public class LookupVectorInTimeSeriesBatchOp extends MapBatchOp <LookupVectorInTimeSeriesBatchOp>
	implements LookupVectorInTimeSeriesParams<LookupVectorInTimeSeriesBatchOp> {

	public LookupVectorInTimeSeriesBatchOp() {
		this(null);
	}

	public LookupVectorInTimeSeriesBatchOp(Params params) {
		super(LookupVectorInTimeSeriesMapper::new, params);
	}
}
