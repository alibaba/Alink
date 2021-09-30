package com.alibaba.alink.operator.batch.image;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.image.ReadImageToTensorMapper;
import com.alibaba.alink.params.image.ReadImageToTensorParams;

public class ReadImageToTensorBatchOp extends MapBatchOp <ReadImageToTensorBatchOp>
	implements ReadImageToTensorParams<ReadImageToTensorBatchOp> {

	public ReadImageToTensorBatchOp() {
		this(new Params());
	}

	public ReadImageToTensorBatchOp(Params params) {
		super(ReadImageToTensorMapper::new, params);
	}
}
