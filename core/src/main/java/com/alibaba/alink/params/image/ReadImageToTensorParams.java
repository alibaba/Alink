package com.alibaba.alink.params.image;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.io.HasRootFilePath;
import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

public interface ReadImageToTensorParams<T>
	extends HasRootFilePath <T>,
	HasOutputCol <T>,
	HasRelativeFilePathCol <T>,
	HasReservedColsDefaultAsNull <T> {
	ParamInfo <Integer> IMAGE_WIDTH = ParamInfoFactory
		.createParamInfo("imageWidth", Integer.class)
		.setDescription("image width")
		.build();

	default Integer getImageWidth() {
		return get(IMAGE_WIDTH);
	}

	default T setImageWidth(Integer width) {
		return set(IMAGE_WIDTH, width);
	}

	ParamInfo <Integer> IMAGE_HEIGHT = ParamInfoFactory
		.createParamInfo("imageHeight", Integer.class)
		.setDescription("image height")
		.build();

	default Integer getImageHeight() {
		return get(IMAGE_HEIGHT);
	}

	default T setImageHeight(Integer height) {
		return set(IMAGE_HEIGHT, height);
	}

}
