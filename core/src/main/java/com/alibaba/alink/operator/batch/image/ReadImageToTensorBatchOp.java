package com.alibaba.alink.operator.batch.image;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.image.ReadImageToTensorMapper;
import com.alibaba.alink.params.image.ReadImageToTensorParams;

@ParamSelectColumnSpec(name = "relativeFilePathCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("图片转张量")
@NameEn("Read Image To Tensor")
public class ReadImageToTensorBatchOp extends MapBatchOp <ReadImageToTensorBatchOp>
	implements ReadImageToTensorParams<ReadImageToTensorBatchOp> {

	public ReadImageToTensorBatchOp() {
		this(new Params());
	}

	public ReadImageToTensorBatchOp(Params params) {
		super(ReadImageToTensorMapper::new, params);
	}
}
