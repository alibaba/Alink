package com.alibaba.alink.operator.local.image;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.image.ReadImageToTensorMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.image.ReadImageToTensorParams;

@ParamSelectColumnSpec(name = "relativeFilePathCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("图片转张量")
public class ReadImageToTensorLocalOp extends MapLocalOp <ReadImageToTensorLocalOp>
	implements ReadImageToTensorParams <ReadImageToTensorLocalOp> {

	public ReadImageToTensorLocalOp() {
		this(new Params());
	}

	public ReadImageToTensorLocalOp(Params params) {
		super(ReadImageToTensorMapper::new, params);
	}
}
