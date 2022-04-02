package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.timeseries.HoltWintersMapper;
import com.alibaba.alink.params.timeseries.HoltWintersParams;

@NameCn("HoltWinters")
public class HoltWintersBatchOp extends MapBatchOp<HoltWintersBatchOp>
	implements HoltWintersParams <HoltWintersBatchOp> {

	private static final long serialVersionUID = 3608585320017729444L;

	public HoltWintersBatchOp() {
		this(new Params());
	}

	public HoltWintersBatchOp(Params params) {
		super(HoltWintersMapper::new, params);
	}

}
