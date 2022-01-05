package com.alibaba.alink.params.image;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

public interface HasImageType<T> extends WithParams <T> {

	/**
	 * @cn-name 图片类型
	 * @cn 图片类型
	 */
	ParamInfo <ImageType> IMAGE_TYPE = ParamInfoFactory
		.createParamInfo("imageType", ImageType.class)
		.setDescription("the handle method of invalid value. include： error, skip")
		.setAlias(new String[] {"handleInvalid"})
		.setHasDefaultValue(ImageType.PNG)
		.build();

	default ImageType getImageType() {
		return get(IMAGE_TYPE);
	}

	default T setImageType(ImageType value) {
		return set(IMAGE_TYPE, value);
	}

	default T setImageType(String value) {
		return set(IMAGE_TYPE, ParamUtil.searchEnum(IMAGE_TYPE, value));
	}

	enum ImageType {
		PNG,
		JPEG
	}
}
