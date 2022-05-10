package com.alibaba.alink.params.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface HasKDEKernelType<T> extends WithParams <T> {
	@NameCn("核密度函数类型")
	@DescCn("核密度函数类型，可取为\"GAUSSIAN\"，\"LINEAR\"")
	ParamInfo <KernelType> KDE_KERNEL_TYPE = ParamInfoFactory
		.createParamInfo("kernelType", KernelType.class)
		.setDescription("kernel type")
		.setHasDefaultValue(KernelType.GAUSSIAN)
		.build();

	default KernelType getKernelType() {return get(KDE_KERNEL_TYPE);}

	default T setKernelType(String value) {return set(KDE_KERNEL_TYPE, ParamUtil.searchEnum(KDE_KERNEL_TYPE, value));}

	default T setKernelType(KernelType value) {return set(KDE_KERNEL_TYPE, value);}

	enum KernelType {
		GAUSSIAN,
		LINEAR
	}
}
