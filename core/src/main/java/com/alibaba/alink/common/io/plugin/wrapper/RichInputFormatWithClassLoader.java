package com.alibaba.alink.common.io.plugin.wrapper;

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.core.io.InputSplit;

import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;

public class RichInputFormatWithClassLoader<T> extends RichInputFormatGenericWithClassLoader <T, InputSplit> {
	public RichInputFormatWithClassLoader(ClassLoaderFactory factory, RichInputFormat<T, InputSplit> inputFormat) {
		super(factory, inputFormat);
	}
}
