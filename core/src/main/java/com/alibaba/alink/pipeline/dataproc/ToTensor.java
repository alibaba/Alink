package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.ToTensorMapper;
import com.alibaba.alink.params.dataproc.ToTensorParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * vector to tensor.
 */
@NameCn("转Tensor")
public class ToTensor extends MapTransformer <ToTensor>
	implements ToTensorParams <ToTensor> {

	public ToTensor() {
		this(new Params());
	}

	public ToTensor(Params params) {
		super(ToTensorMapper::new, params);
	}

}
