package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.timeseries.ProphetMapper;
import com.alibaba.alink.params.timeseries.ProphetParams;

@NameCn("Prophet")
@NameEn("Prophet")
public class ProphetBatchOp extends MapBatchOp <ProphetBatchOp>
	implements ProphetParams <ProphetBatchOp> {

	public ProphetBatchOp() {
		this(null);
	}

	public ProphetBatchOp(Params params) {
		super(ProphetMapper::new, params);
	}

}
