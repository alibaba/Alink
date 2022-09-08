package com.alibaba.alink.operator.local.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.ToMTableMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.dataproc.ToMTableParams;

/**
 * batch op for transforming to MTable.
 */
@NameCn("转MTable")
public class ToMTableLocalOp extends MapLocalOp <ToMTableLocalOp>
	implements ToMTableParams <ToMTableLocalOp> {

	public ToMTableLocalOp() {
		this(new Params());
	}

	public ToMTableLocalOp(Params params) {
		super(ToMTableMapper::new, params);
	}

}
