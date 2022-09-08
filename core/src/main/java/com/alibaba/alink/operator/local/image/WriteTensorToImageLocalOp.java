package com.alibaba.alink.operator.local.image;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.image.WriteTensorToImageMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.image.WriteTensorToImageParams;

@ParamSelectColumnSpec(name = "tensorCol", allowedTypeCollections = TypeCollections.TENSOR_TYPES)
@ParamSelectColumnSpec(name = "relativeFilePathCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("张量转图片")
public class WriteTensorToImageLocalOp extends MapLocalOp <WriteTensorToImageLocalOp>
	implements WriteTensorToImageParams <WriteTensorToImageLocalOp> {

	public WriteTensorToImageLocalOp() {
		this(new Params());
	}

	public WriteTensorToImageLocalOp(Params params) {
		super(WriteTensorToImageMapper::new, params);
	}
}
