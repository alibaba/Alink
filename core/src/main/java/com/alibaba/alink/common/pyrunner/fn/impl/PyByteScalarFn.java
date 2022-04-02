package com.alibaba.alink.common.pyrunner.fn.impl;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.pyrunner.fn.BasePyScalarFn;
import com.alibaba.alink.common.pyrunner.fn.PyScalarFnHandle;
import com.alibaba.alink.common.utils.Functional.SerializableBiFunction;

import java.util.Collections;

@Internal
public class PyByteScalarFn extends BasePyScalarFn <Byte, PyScalarFnHandle <Byte>> {

	public PyByteScalarFn(String name, String fnSpecJson) {
		this(name, fnSpecJson, Collections. <String, String>emptyMap()::getOrDefault);
	}

	public PyByteScalarFn(String name, String fnSpecJson,
						  SerializableBiFunction <String, String, String> runConfigGetter) {
		super(name, fnSpecJson, byte.class, runConfigGetter);
	}

	@Override
	public TypeInformation <?> getResultType(Class <?>[] signature) {
		return Types.BYTE;
	}

	public byte eval(Object... args) {
		return runner.calc(args);
	}
}
