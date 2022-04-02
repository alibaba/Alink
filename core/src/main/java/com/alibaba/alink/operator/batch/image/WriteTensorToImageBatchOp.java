package com.alibaba.alink.operator.batch.image;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.image.WriteTensorToImageMapper;
import com.alibaba.alink.params.image.WriteTensorToImageParams;

@ParamSelectColumnSpec(name = "tensorCol", allowedTypeCollections = TypeCollections.TENSOR_TYPES)
@ParamSelectColumnSpec(name = "relativeFilePathCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("张量转图片")
public class WriteTensorToImageBatchOp extends MapBatchOp <WriteTensorToImageBatchOp>
	implements WriteTensorToImageParams<WriteTensorToImageBatchOp> {

	public WriteTensorToImageBatchOp() {
		this(new Params());
	}

	public WriteTensorToImageBatchOp(Params params) {
		super(WriteTensorToImageMapper::new, params);
	}
}
