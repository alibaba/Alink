package com.alibaba.alink.operator.stream.image;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.image.WriteTensorToImageMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.image.WriteTensorToImageParams;

@ParamSelectColumnSpec(name = "tensorCol", allowedTypeCollections = TypeCollections.TENSOR_TYPES)
@ParamSelectColumnSpec(name = "relativeFilePathCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("张量转图片")
public class WriteTensorToImageStreamOp extends MapStreamOp <WriteTensorToImageStreamOp>
	implements WriteTensorToImageParams<WriteTensorToImageStreamOp> {

	public WriteTensorToImageStreamOp() {
		this(new Params());
	}

	public WriteTensorToImageStreamOp(Params params) {
		super(WriteTensorToImageMapper::new, params);
	}
}
