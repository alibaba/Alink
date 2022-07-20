package com.alibaba.alink.common.pyrunner.fn.impl;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.pyrunner.fn.BasePyScalarFn;
import com.alibaba.alink.common.pyrunner.fn.PyScalarFnHandle;

import java.util.Collections;
import java.util.Map;

@Internal
public class PyByteScalarFn extends BasePyScalarFn <Byte, PyScalarFnHandle <Byte>> {

	public PyByteScalarFn(String name, String fnSpecJson) {
		this(name, fnSpecJson, Collections.emptyMap());
	}

	public PyByteScalarFn(String name, String fnSpecJson, Map <String, String> runConfig) {
		super(name, fnSpecJson, byte.class, runConfig);
	}

	@Override
	public TypeInformation <?> getResultType(Class <?>[] signature) {
		return Types.BYTE;
	}

	public byte eval(Object... args) {
		return runner.calc(args);
	}
}
