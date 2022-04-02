package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.timeseries.AutoGarchMapper;
import com.alibaba.alink.params.timeseries.AutoGarchParams;

@NameCn("AutoGarch")
public final class AutoGarchBatchOp extends MapBatchOp<AutoGarchBatchOp>
	implements AutoGarchParams <AutoGarchBatchOp> {

	private static final long serialVersionUID = 3545250121467156432L;

	public AutoGarchBatchOp() {
		this(null);
	}

	public AutoGarchBatchOp(Params params) {
		super(AutoGarchMapper::new, params);
	}
}
