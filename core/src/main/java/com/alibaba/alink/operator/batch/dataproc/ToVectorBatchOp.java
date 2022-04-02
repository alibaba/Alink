package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.ToVectorMapper;
import com.alibaba.alink.params.dataproc.ToVectorParams;

/**
 * batch op for transforming to vector.
 */
@NameCn("转向量")
public class ToVectorBatchOp extends MapBatchOp <ToVectorBatchOp>
	implements ToVectorParams <ToVectorBatchOp> {

	public ToVectorBatchOp() {
		this(new Params());
	}

	public ToVectorBatchOp(Params params) {
		super(ToVectorMapper::new, params);
	}

}
