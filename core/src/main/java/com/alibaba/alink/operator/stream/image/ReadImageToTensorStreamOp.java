package com.alibaba.alink.operator.stream.image;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.image.ReadImageToTensorMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.image.ReadImageToTensorParams;

@ParamSelectColumnSpec(name = "relativeFilePathCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("图片转张量")
public class ReadImageToTensorStreamOp extends MapStreamOp <ReadImageToTensorStreamOp>
	implements ReadImageToTensorParams<ReadImageToTensorStreamOp> {

	public ReadImageToTensorStreamOp() {
		this(new Params());
	}

	public ReadImageToTensorStreamOp(Params params) {
		super(ReadImageToTensorMapper::new, params);
	}
}
