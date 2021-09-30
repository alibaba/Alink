package com.alibaba.alink.operator.stream.image;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.image.WriteTensorToImageMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.image.WriteTensorToImageParams;

public class WriteTensorToImageStreamOp extends MapStreamOp <WriteTensorToImageStreamOp>
	implements WriteTensorToImageParams<WriteTensorToImageStreamOp> {

	public WriteTensorToImageStreamOp() {
		this(new Params());
	}

	public WriteTensorToImageStreamOp(Params params) {
		super(WriteTensorToImageMapper::new, params);
	}
}
