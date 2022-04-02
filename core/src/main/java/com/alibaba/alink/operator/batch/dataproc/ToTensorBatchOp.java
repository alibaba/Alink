package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.ToTensorMapper;
import com.alibaba.alink.params.dataproc.ToTensorParams;

/**
 * batch op for transforming to tensor.
 */
@NameCn("转Tensor")
public class ToTensorBatchOp extends MapBatchOp <ToTensorBatchOp>
	implements ToTensorParams <ToTensorBatchOp> {

	public ToTensorBatchOp() {
		this(new Params());
	}

	public ToTensorBatchOp(Params params) {
		super(ToTensorMapper::new, params);
	}

}
