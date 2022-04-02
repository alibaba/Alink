package com.alibaba.alink.params.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface HaskernelType<T> extends WithParams <T> {

	@NameCn("核函数类型")
	@DescCn("核函数类型，可取为\"RBF\"，\"POLY\"，\"SIGMOID\"，\"LINEAR\"")
	ParamInfo <KernelType> KERNEL_TYPE = ParamInfoFactory
		.createParamInfo("kernelType", KernelType.class)
		.setDescription("kernel type")
		.setHasDefaultValue(KernelType.RBF)
		.build();

	default KernelType getKernelType() {return get(KERNEL_TYPE);}

	default T setKernelType(String value) {return set(KERNEL_TYPE, ParamUtil.searchEnum(KERNEL_TYPE, value));}

	default T setKernelType(KernelType value) {return set(KERNEL_TYPE, value);}

	enum KernelType {
		RBF,
		POLY,
		SIGMOID,
		LINEAR
	}
}
