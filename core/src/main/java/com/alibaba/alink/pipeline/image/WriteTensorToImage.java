package com.alibaba.alink.pipeline.image;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.image.WriteTensorToImageMapper;
import com.alibaba.alink.params.image.WriteTensorToImageParams;
import com.alibaba.alink.pipeline.MapTransformer;

@NameCn("张量转图片")
public class WriteTensorToImage extends MapTransformer <WriteTensorToImage>
	implements WriteTensorToImageParams <WriteTensorToImage> {

	public WriteTensorToImage() {
		this(null);
	}

	public WriteTensorToImage(Params params) {
		super(WriteTensorToImageMapper::new, params);
	}
}
