package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.shared.HasRandomSeed;
import com.alibaba.alink.params.validators.RangeValidator;

public interface SplitParams<T> extends
	WithParams <T>,
	HasRandomSeed <T> {

	@NameCn("拆分到左端的数据比例")
	@DescCn("拆分到左端的数据比例")
	ParamInfo <Double> FRACTION = ParamInfoFactory
		.createParamInfo("fraction", Double.class)
		.setDescription("Proportion of data allocated to left output after splitting")
		.setRequired()
		.setValidator(new RangeValidator <>(0.0, 1.0))
		.build();

	default Double getFraction() {
		return get(FRACTION);
	}

	default T setFraction(Double fraction) {
		return set(FRACTION, fraction);
	}
}
