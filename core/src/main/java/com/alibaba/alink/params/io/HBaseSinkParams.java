package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.dataproc.HasValueCols;

public interface HBaseSinkParams<T> extends
	HBaseConfigParams <T>,
	HBaseParams <T>,
	HasValueCols <T> {}
