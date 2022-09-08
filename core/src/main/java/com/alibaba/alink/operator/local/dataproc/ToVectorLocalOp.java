package com.alibaba.alink.operator.local.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.ToVectorMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.dataproc.ToVectorParams;

/**
 * local op for transforming to vector.
 */
@NameCn("转向量")
public class ToVectorLocalOp extends MapLocalOp <ToVectorLocalOp>
	implements ToVectorParams <ToVectorLocalOp> {

	public ToVectorLocalOp() {
		this(new Params());
	}

	public ToVectorLocalOp(Params params) {
		super(ToVectorMapper::new, params);
	}

}
