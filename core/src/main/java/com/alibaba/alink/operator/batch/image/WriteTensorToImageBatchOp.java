package com.alibaba.alink.operator.batch.image;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.image.WriteTensorToImageMapper;
import com.alibaba.alink.params.image.WriteTensorToImageParams;

public class WriteTensorToImageBatchOp extends MapBatchOp <WriteTensorToImageBatchOp>
	implements WriteTensorToImageParams<WriteTensorToImageBatchOp> {

	public WriteTensorToImageBatchOp() {
		this(new Params());
	}

	public WriteTensorToImageBatchOp(Params params) {
		super(WriteTensorToImageMapper::new, params);
	}
}
