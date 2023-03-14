package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.timeseries.ShiftMapper;
import com.alibaba.alink.params.timeseries.ShiftParams;

@NameCn("Shift")
@NameEn("Shift")
public class ShiftBatchOp extends MapBatchOp <ShiftBatchOp>
	implements ShiftParams<ShiftBatchOp> {

	public ShiftBatchOp() {
		this(null);
	}

	public ShiftBatchOp(Params params) {
		super(ShiftMapper::new, params);
	}

}
