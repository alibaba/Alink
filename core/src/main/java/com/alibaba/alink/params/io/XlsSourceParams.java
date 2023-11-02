package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface XlsSourceParams<T> extends WithParams <T>,
	HasFilePath <T>, HasIgnoreFirstLine <T>, HasSchemaStr <T> ,HasLenient<T>{
	@NameCn("表格的Sheet编号")
	@DescCn("表格的Sheet编号")
	ParamInfo <Integer> SHEET_INDEX = ParamInfoFactory
		.createParamInfo("sheetIndex", Integer.class)
		.setDescription("read selected sheet from workbook.")
		.setAlias(new String[] {"sheetId"})
		.setHasDefaultValue(0)
		.build();

	default Integer getSheetIndex() {
		return get(SHEET_INDEX);
	}

	default T setSheetIndex(Integer value) {
		return set(SHEET_INDEX, value);
	}
}
