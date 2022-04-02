package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.ToVectorMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.ToVectorParams;

/**
 * stream op for transforming to vector.
 */
@NameCn("转向量")
public class ToVectorStreamOp extends MapStreamOp <ToVectorStreamOp>
	implements ToVectorParams <ToVectorStreamOp> {

	public ToVectorStreamOp() {
		this(new Params());
	}

	public ToVectorStreamOp(Params params) {
		super(ToVectorMapper::new, params);
	}

}
