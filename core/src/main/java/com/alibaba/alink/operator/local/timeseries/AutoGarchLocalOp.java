package com.alibaba.alink.operator.local.timeseries;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.timeseries.AutoGarchMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.timeseries.AutoGarchParams;

@NameCn("AutoGarch")
public final class AutoGarchLocalOp extends MapLocalOp <AutoGarchLocalOp>
	implements AutoGarchParams <AutoGarchLocalOp> {

	private static final long serialVersionUID = 3545250121467156432L;

	public AutoGarchLocalOp() {
		this(null);
	}

	public AutoGarchLocalOp(Params params) {
		super(AutoGarchMapper::new, params);
	}
}
