package com.alibaba.alink.operator.stream.image;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.image.ReadImageToTensorMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.image.ReadImageToTensorParams;

public class ReadImageToTensorStreamOp extends MapStreamOp <ReadImageToTensorStreamOp>
	implements ReadImageToTensorParams<ReadImageToTensorStreamOp> {

	public ReadImageToTensorStreamOp() {
		this(new Params());
	}

	public ReadImageToTensorStreamOp(Params params) {
		super(ReadImageToTensorMapper::new, params);
	}
}
