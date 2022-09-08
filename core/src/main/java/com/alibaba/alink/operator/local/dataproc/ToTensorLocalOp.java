package com.alibaba.alink.operator.local.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.ToTensorMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.dataproc.ToTensorParams;

/**
 * local op for transforming to tensor.
 */
@NameCn("转Tensor")
public class ToTensorLocalOp extends MapLocalOp <ToTensorLocalOp>
	implements ToTensorParams <ToTensorLocalOp> {

	public ToTensorLocalOp() {
		this(new Params());
	}

	public ToTensorLocalOp(Params params) {
		super(ToTensorMapper::new, params);
	}

}
